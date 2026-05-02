package storage

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	// maxHeight — максимально возможная высота узла.
	// При branchFactor=4 это покрывает до 4^20 ≈ 10^12 элементов.
	// На практике memtable содержит миллионы записей — высоты 20 более чем достаточно.
	maxHeight = 20

	// branchFactor — определяет вероятность подъёма узла на следующий уровень.
	// Вероятность = 1/branchFactor = 1/4 = 25%.
	// LevelDB использует тот же множитель.
	//
	// Чем больше branchFactor:
	//   - меньше уровней → меньше памяти на указатели
	//   - но больше узлов на каждом уровне → чуть медленнее поиск
	// branchFactor = 4 — хороший баланс между памятью и скоростью.
	branchFactor = 4
)

// skipListNode — один узел skip list.
//
// Каждый узел хранит ключ, значение и массив указателей next[].
// next[i] указывает на следующий узел на уровне i.
// Высота узла (сколько уровней он занимает) определяется случайно при вставке.
//
//   Пример узла с высотой 3:
//
//   ┌──────────────────────┐
//   │  key:   []byte("12") │
//   │  value: []byte("v1") │
//   │                      │
//   │  next[2]: ───────────────► следующий узел на уровне 2
//   │  next[1]: ───────────────► следующий узел на уровне 1
//   │  next[0]: ───────────────► следующий узел на уровне 0
//   │  next[3..19]: nil     │   (неиспользуемые уровни)
//   └──────────────────────┘
//
// Массив next имеет фиксированный размер maxHeight. Это проще, чем
// динамический слайс, и позволяет избежать лишних аллокаций.
// Неиспользуемые уровни остаются nil.
type skipListNode struct {
	key   []byte
	value []byte
	next  [maxHeight]*skipListNode
}

// SkipList — отсортированная in-memory структура данных.
//
// Используется как memtable в LSM-tree: все записи попадают сюда,
// а когда byteSize превышает порог — содержимое сбрасывается
// на диск в виде SSTable.
//
//   Общая структура:
//
//   ┌──────────────────────────────────────────────────────────┐
//   │  SkipList                                                │
//   │                                                          │
//   │  height: 3  (текущая максимальная высота)                │
//   │  length: 6  (количество элементов)                       │
//   │  byteSize: 128  (суммарный размер ключей + значений)     │
//   │                                                          │
//   │  Level 2:  head ──────────── 12 ──────────── 35 ── NIL   │
//   │              │                │               │          │
//   │  Level 1:  head ───── 7 ──── 12 ──── 26 ──── 35 ── NIL   │
//   │              │        │       │       │       │          │
//   │  Level 0:  head → 3 → 7 → 12 → 19 → 26 → 35 → 42 → NIL   │
//   └──────────────────────────────────────────────────────────┘
//
// head — фиктивный узел (sentinel). Он не хранит данных, а служит
// точкой входа для всех уровней. Благодаря ему не нужно обрабатывать
// вставку в начало списка как особый случай.
type SkipList struct {
	mu       sync.RWMutex
	head     skipListNode
	height   int // Текущая максимальная высота
	length   int
	byteSize int64 // Суммарный размер ключей + значений (для flush threshold)
}

func NewSkipList() *SkipList {
	return &SkipList{height: 1}
}

// Set вставляет или обновляет ключ.
//
// Алгоритм состоит из четырёх шагов:
//
//  1. Найти позицию вставки на каждом уровне (массив update[])
//  2. Проверить: если ключ уже есть → обновить value
//  3. Выбрать случайную высоту нового узла
//  4. Создать узел и вставить на всех уровнях
//
// Шаг 1 подробно:
//
//   Спускаемся от верхнего уровня к нижнему. На каждом уровне
//   двигаемся вправо, пока следующий ключ < искомого.
//   Последний узел на каждом уровне запоминаем в update[i] —
//   это узлы, чьи указатели нужно будет обновить при вставке.
//
//   Пример: Set("20") в skip list с ключами 12, 19, 26, 35
//
//   Level 2:  head ──────────── 12 ──────────── 35 ── NIL
//                               ▲ update[2]
//   Level 1:  head ──────────── 12 ──── 26 ──── 35 ── NIL
//                               ▲ update[1]
//   Level 0:  head ──────────── 12 → 19 → 26 → 35 ── NIL
//                                    ▲ update[0]
//
//   update = [19, 12, 12, ...]
//   (на уровне 0 последний узел с ключом < 20 — это 19,
//    на уровнях 1 и 2 — это 12)
func (sl *SkipList) Set(key, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// --- Шаг 1: находим позицию вставки на каждом уровне ---
	//
	// update[i] — последний узел на уровне i, чей ключ < key.
	// Новый узел будет вставлен сразу после update[i] на каждом уровне.
	update := [maxHeight]*skipListNode{}
	curr := &sl.head
	for i := sl.height - 1; i >= 0; i-- {
		// Двигаемся вправо на уровне i, пока следующий ключ < key.
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
		// curr — последний узел на уровне i с ключом < key.
		update[i] = curr
	}

	// --- Шаг 2: проверяем, есть ли ключ уже ---
	//
	// После цикла curr.next[0] — это либо узел с ключом >= key, либо nil.
	// Если ключ совпадает — обновляем value на месте, без создания нового узла.
	if curr.next[0] != nil && bytes.Compare(curr.next[0].key, key) == 0 {
		node := curr.next[0]
		oldValLen := len(node.value)

		// Копируем value, чтобы skip list владел своей копией данных.
		// nil value означает tombstone (ключ помечен как удалённый).
		if value != nil {
			node.value = append([]byte{}, value...)
		} else {
			node.value = nil
		}

		// Корректируем byteSize: вычитаем старый размер, добавляем новый.
		sl.byteSize += int64(len(node.value)) - int64(oldValLen)
		return
	}

	// --- Шаг 3: выбираем случайную высоту нового узла ---
	//
	// Если новая высота больше текущей максимальной — для новых уровней
	// update[i] указывает на head (они пока пусты, новый узел будет первым).
	h := sl.randomHeight()
	if h > sl.height {
		for i := sl.height; i < h; i++ {
			update[i] = &sl.head
		}
		sl.height = h
	}

	// --- Шаг 4: создаём узел и вставляем на всех уровнях ---
	//
	// Создаём копии key и value, чтобы skip list не зависел
	// от внешних слайсов (вызывающий код может их переиспользовать).
	node := &skipListNode{
		key: append([]byte{}, key...),
	}
	if value != nil {
		node.value = append([]byte{}, value...)
	}

	// Вставляем узел на каждом уровне от 0 до h-1.
	//
	//   До вставки (уровень i):
	//     update[i] ──────────────► update[i].next[i]
	//
	//   После вставки (уровень i):
	//     update[i] ──► node ──► update[i].next[i]  (бывший next)
	//
	// Два действия на каждом уровне:
	//   1. Новый узел указывает на то, что было после update[i]
	//   2. update[i] теперь указывает на новый узел
	for i := 0; i < h; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	sl.length++
	sl.byteSize += int64(len(key)) + int64(len(value))
}

// Get возвращает значение для ключа или (nil, false) если не найдено.
//
// Алгоритм: спускаемся от верхнего уровня к нижнему,
// на каждом уровне двигаемся вправо, пока можем.
// На уровне 0 проверяем, совпадает ли ключ.
//
//   Get("26"):
//
//   Level 2:  head ──── 12 ──── 35    35 > 26, спускаемся с 12
//   Level 1:  ......... 12 ──── 26    26 == 26, но проверка будет на уровне 0
//   Level 0:  ......... 12 → 19 → 26  ← нашли!
//
// Возвращает копию? Нет — возвращает ссылку на внутренний слайс.
// Вызывающий код не должен модифицировать результат.
func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := &sl.head
	// Спускаемся по уровням от верхнего к нижнему.
	// На каждом уровне продвигаемся вправо, пока следующий ключ < key.
	for i := sl.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}

	// После цикла curr.next[0] — первый узел с ключом >= key.
	// Если ключ совпадает — нашли.
	if curr.next[0] != nil && bytes.Compare(curr.next[0].key, key) == 0 {
		return curr.next[0].value, true
	}

	return nil, false
}

// Delete помечает ключ как удалённый, записывая tombstone (value = nil).
//
// Физически узел остаётся в skip list — это важно для LSM-tree:
// при flush в SSTable tombstone записывается как маркер удаления,
// чтобы при compaction удалить ключ из старых SSTable.
func (sl *SkipList) Delete(key []byte) {
	sl.Set(key, nil)
}

// KV — пара ключ-значение, используется в RangeScan.
type KV struct {
	Key, Value []byte
}

// RangeScan возвращает все пары ключ-значение с ключами в диапазоне [start, end).
//
// Пример: RangeScan("user:100", "user:200") вернёт все ключи
// от "user:100" (включительно) до "user:200" (не включительно).
//
//   Как это работает:
//
//   1. Seek(start) — позиционируем итератор на первый ключ >= start
//      Используем уровни skip list для быстрого поиска — O(log n).
//
//   2. Идём по уровню 0 вправо, собирая пары, пока ключ < end.
//      Это O(k), где k — количество результатов.
//
//   ┌──────────────────────────────────────────────────┐
//   │ Level 0: ... → 99 → 100 → 105 → 150 → 200 → ...  │
//   │                      ▲                  ▲        │
//   │                    start              end        │
//   │                      |←── собираем ──►|          │
//   └──────────────────────────────────────────────────┘
//
// Возвращает копии ключей и значений — безопасно модифицировать.
func (sl *SkipList) RangeScan(start, end []byte) []KV {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Создаём итератор и позиционируем на первый ключ >= start.
	iterator := sl.newIterator()
	iterator.seek(start)

	var result []KV
	for iterator.Valid() && bytes.Compare(iterator.Key(), end) < 0 {
		// Копируем ключ и значение, чтобы результат не зависел
		// от внутреннего состояния skip list.
		kv := KV{Key: append([]byte{}, iterator.Key()...)}
		if iterator.Value() != nil {
			kv.Value = append([]byte{}, iterator.Value()...)
		}
		result = append(result, kv)
		iterator.Next()
	}
	return result
}

// randomHeight генерирует случайную высоту для нового узла.
//
// С вероятностью 1/branchFactor (= 1/4) узел поднимается
// на каждый следующий уровень. Это похоже на подбрасывание
// нечестной монеты: «орёл» (1/4) — ещё уровень,
// «решка» (3/4) — останавливаемся.
//
//   Распределение высот:
//   Высота 1:  ~75%
//   Высота 2:  ~18.75%
//   Высота 3:  ~4.69%
//   Высота 4:  ~1.17%
//   ...
func (sl *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Intn(branchFactor) == 0 {
		h++
	}
	return h
}

// SkipListIterator — итератор по ключам skip list в отсортированном порядке.
//
// Используется для двух целей:
//   1. Flush memtable → SSTable: проход по всем ключам в порядке сортировки
//   2. Range scan: проход по диапазону ключей
//
// Итератор ходит по уровню 0 (где есть все элементы):
//
//   Level 0:  head → 3 → 7 → 12 → 19 → 26 → 35 → 42 → NIL
//                    ▲
//                    current (начальная позиция после NewIterator)
//
// Важно: методы Next, Key, Value НЕ берут лок. Это означает, что
// итерация одновременно с записью (Set) — формальный data race.
// На практике это безопасно в нашей реализации, потому что узлы
// никогда не удаляются физически и указатели next[0] перезаписываются
// атомарно на уровне CPU. Но для production кода это не годится —
// нужен либо lock-free skip list (как в Pebble: atomic CAS на указателях),
// либо удержание RLock на всё время жизни итератора.
type SkipListIterator struct {
	sl      *SkipList
	current *skipListNode
}

func (sl *SkipList) NewIterator() *SkipListIterator {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.newIterator()
}

// newIterator — внутренний метод без лока. Вызывающий код должен держать mu.RLock.
func (sl *SkipList) newIterator() *SkipListIterator {
	return &SkipListIterator{sl: sl, current: sl.head.next[0]}
}

func (it *SkipListIterator) Valid() bool {
	return it.current != nil
}

func (it *SkipListIterator) Key() []byte {
	return it.current.key
}

func (it *SkipListIterator) Value() []byte {
	return it.current.value
}

// Next перемещает итератор к следующему элементу на уровне 0.
//
//   До:  ... → [current] → [next] → ...
//   После: ... → [prev] → [current] → ...
//                           (бывший next)
func (it *SkipListIterator) Next() {
	it.current = it.current.next[0]
}

// seek — внутренний метод без лока.
// Вызывающий код должен держать mu.RLock.
//
// Алгоритм аналогичен Get: спускаемся по уровням,
// на каждом двигаемся вправо пока можем.
// После цикла curr.next[0] — первый узел с ключом >= key.
//
//   seek("20"):
//
//   Level 2:  head ──── 12 ──── 35      35 > 20, спускаемся с 12
//   Level 1:  ......... 12 ──── 26      26 > 20, спускаемся с 12
//   Level 0:  ......... 12 → 19 → 26    19 < 20, идём. 26 >= 20, стоп.
//                              ▲
//                        curr (= узел 19)
//
//   curr.next[0] = узел 26 → это первый ключ >= 20
//   it.current = узел 26
func (it *SkipListIterator) Seek(key []byte) {
	it.sl.mu.RLock()
	defer it.sl.mu.RUnlock()
	it.seek(key)
}

// seek — внутренний метод без лока. Вызывающий код должен держать mu.RLock.
func (it *SkipListIterator) seek(key []byte) {
	curr := &it.sl.head
	for i := it.sl.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].key, key) < 0 {
			curr = curr.next[i]
		}
	}
	it.current = curr.next[0]
}
