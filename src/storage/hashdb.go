package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

// ErrCorrupted возвращается из Get, если CRC записи не совпадает.
// Это означает, что данные на диске повреждены — либо битый сектор,
// либо неполная запись после краша.
var ErrCorrupted = errors.New("record corrupted: CRC mismatch")

// valueLocation описывает положение записи в файле.
//
//   offset — смещение от начала файла (в байтах) до первого байта записи (CRC32).
//   size   — полный размер записи, включая заголовок и данные.
//
//   ┌──────────────────────── файл ────────────────────────────┐
//   │          ...          │◄── size ──►│         ...         │
//   │                       │            │                     │
//   │                offset ▼            │                     │
//   │                       ┌────────────┐                     │
//   │                       │CRC|kl|vl|k|v                     │
//   │                       └────────────┘                     │
//   └──────────────────────────────────────────────────────────┘
//
// Зная offset и size, можно прочитать запись одним вызовом ReadAt,
// без предварительного парсинга заголовка и без Seek.
type valueLocation struct {
	offset int64
	size   int32
}

// HashDB — key-value хранилище с in-memory hash index поверх append-only log.
//
// Архитектура:
//
//   ┌──────────────────────────────────┐
//   │         HashDB (in-memory)       │
//   │                                  │
//   │   index: map[string]valueLocation│
//   │   ┌────────────────────────────┐ │
//   │   │ "name" → {offset:0,  s:23} │ │
//   │   │ "age"  → {offset:42, s:19} │ │
//   │   │ "city" → {offset:81, s:24} │ │
//   │   └────────┬───────────────────┘ │
//   └────────────┼─────────────────────┘
//                │ ReadAt(buf, offset)
//                ▼
//   ┌──────────────────────────────────┐
//   │         Log (файл на диске)      │
//   │  [rec0][rec1][rec2][rec3]...     │
//   └──────────────────────────────────┘
//
// Set/Delete дописывают записи в конец файла через Log.
// Get читает ровно одну запись по offset из индекса.
// При старте индекс восстанавливается полным сканом файла.
type HashDB struct {
	mu    sync.RWMutex
	log   *Log
	index map[string]valueLocation // key → где в файле лежит последняя версия
}

// OpenHashDB открывает (или создаёт) хранилище по указанному пути.
//
// Порядок действий:
//  1. Открыть append-only log (файл)
//  2. Создать пустой индекс
//  3. Просканировать файл и восстановить индекс (buildIndex)
//
// После возврата из OpenHashDB хранилище готово к работе:
// индекс уже содержит актуальные offsets для всех живых ключей.
func OpenHashDB(path string) (*HashDB, error) {
	log, err := Open(path)
	if err != nil {
		return nil, err
	}

	db := &HashDB{
		log:   log,
		index: make(map[string]valueLocation),
	}

	// При открытии восстанавливаем индекс из файла.
	// Это O(n) от размера файла — только при старте.
	if err := db.buildIndex(); err != nil {
		return nil, err
	}

	return db, nil
}

// buildIndex сканирует весь файл и заполняет in-memory индекс.
//
// Для каждой записи в файле:
//   - Если value == tombstone → удаляем ключ из индекса
//   - Иначе → записываем {offset, size} в индекс
//
// Поскольку файл читается от начала к концу, а в индексе сохраняется
// последнее значение для каждого ключа, в итоге индекс содержит
// только актуальные записи.
//
//   Файл:
//   offset=0         offset=23        offset=42
//   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
//   │ name→alice  │  │ age→25      │  │ name→bob    │
//   └─────────────┘  └─────────────┘  └─────────────┘
//
//   Шаг 1: index["name"] = {offset:0,  size:23}
//   Шаг 2: index["age"]  = {offset:23, size:19}
//   Шаг 3: index["name"] = {offset:42, size:19}  ← перезапись
//
//   Итог: index = { "name": {42, 19}, "age": {23, 19} }
func (db *HashDB) buildIndex() error {
	return db.log.scan(func(key, value []byte, offset int64) error {
		if string(value) == tombstoneVal {
			delete(db.index, string(key))
		} else {
			db.index[string(key)] = valueLocation{
				offset: offset,
				size:   int32(recordHeaderSize + len(key) + len(value)),
			}
		}

		return nil
	})
}

// Set записывает пару ключ-значение и обновляет индекс.
//
// Порядок действий:
//  1. Узнаём текущую позицию конца файла — это будет offset новой записи
//  2. Дописываем запись в файл через Log
//  3. Обновляем индекс: index[key] = {offset, size}
//
// Если ключ уже существовал — старая запись остаётся в файле (мёртвые данные),
// но индекс теперь указывает на новую. Старая запись станет мусором,
// который можно удалить только через compaction (в этой реализации не делаем).
//
//   До Set("name", "bob"):
//     index["name"] = {offset: 0, size: 23}  →  файл[0]: name→alice
//
//   После Set("name", "bob"):
//     index["name"] = {offset: 42, size: 22}  →  файл[42]: name→bob
//     файл[0]: name→alice  ← мёртвая запись, на неё никто не ссылается
func (db *HashDB) Set(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Seek(0, SeekEnd) возвращает текущий размер файла = offset,
	// по которому будет записана новая запись.
	// Примечание: файл открыт с O_APPEND, поэтому Write всегда
	// дописывает в конец. Но Seek нужен именно чтобы узнать offset,
	// а не чтобы переместить позицию записи.
	offset, err := db.log.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if err := db.log.writeRecord(key, value); err != nil {
		return err
	}

	// Вычисляем полный размер записи на диске:
	//
	//   ┌──────────┬─────────┬─────────┬─────────┬───────────┐
	//   │  CRC32   │ key_len │ val_len │   key   │   value   │
	//   │  4 bytes │ 4 bytes │ 4 bytes │  N B    │   M B     │
	//   └──────────┴─────────┴─────────┴─────────┴───────────┘
	//   |◄──────── recordHeaderSize ──►|
	//   |◄──────────────── size ────────────────────────────►|
	size := int32(recordHeaderSize + len(key) + len(value))

	db.index[string(key)] = valueLocation{
		offset: offset,
		size:   size,
	}

	return nil
}

// Get читает значение по ключу за O(1).
//
// Порядок действий:
//  1. Ищем ключ в hash table → получаем {offset, size}
//  2. Читаем size байт из файла по offset (ReadAt — без Seek)
//  3. Проверяем CRC для целостности
//  4. Извлекаем value из прочитанных байтов
//
//   index["name"] = {offset: 42, size: 22}
//                          │
//                          ▼
//   Файл: ...[offset=42]┌──────┬────┬────┬──────┬─────┐
//                       │ CRC  │ 4  │ 3  │ name │ bob │
//                       └──────┴────┴────┴──────┴─────┘
//                       |◄──────── 22 байт ──────────►|
//                                                ▲
//                                                возвращаем "bob"
func (db *HashDB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Шаг 1: lookup в hash table — O(1).
	loc, ok := db.index[string(key)]
	if !ok {
		return nil, ErrNotFound
	}

	// Шаг 2: читаем всю запись одним вызовом ReadAt.
	// ReadAt читает ровно len(buf) байт начиная с позиции loc.offset.
	// В отличие от Read + Seek, ReadAt не меняет позицию файлового
	// дескриптора — это безопасно для concurrent reads.
	buf := make([]byte, loc.size)

	if _, err := db.log.file.ReadAt(buf, loc.offset); err != nil {
		return nil, err
	}

	// Шаг 3: проверяем целостность.
	//
	//   buf: [CRC32 | key_len | val_len | key | value]
	//         0:4     4:8       8:12      ...   ...
	//         ▲     |◄── payload (всё после CRC) ──►|
	//         │
	//    checksum
	//
	// Вычисляем CRC от payload (buf[4:]) и сравниваем с сохранённым (buf[0:4]).
	checksum := binary.LittleEndian.Uint32(buf[0:4])
	payload := buf[4:]
	if crc32.ChecksumIEEE(payload) != checksum {
		return nil, ErrCorrupted
	}

	// Шаг 4: извлекаем value.
	//
	//   buf: [CRC32 | key_len | val_len | key        | value     ]
	//        0       4         8         12           12+keyLen
	//                                  |◄─ keyLen ─►|◄─ value ─►|
	keyLen := binary.LittleEndian.Uint32(buf[4:8])
	value := buf[recordHeaderSize+keyLen:]

	return value, nil
}

// Delete помечает ключ как удалённый.
//
// Порядок действий:
//  1. Записываем tombstone в файл (обычная запись с value = tombstoneVal)
//  2. Удаляем ключ из индекса
//
// После удаления из индекса ключ перестаёт быть видимым для Get.
// Физически в файле остаются все старые записи + tombstone — это мусор,
// который накапливается до compaction.
func (db *HashDB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Записываем tombstone в файл. Это нужно для корректного
	// восстановления индекса при рестарте: buildIndex увидит tombstone
	// и не добавит ключ в индекс.
	if err := db.log.writeRecord(key, []byte(tombstoneVal)); err != nil {
		return err
	}

	// Удаляем из in-memory индекса. Теперь Get вернёт ErrNotFound.
	delete(db.index, string(key))

	return nil
}

func (db *HashDB) Close() error {
	return db.log.Close()
}

// Stats возвращает статистику для понимания накопленного мусора.
//
//   LiveKeys       — количество живых ключей в индексе
//   TotalFileBytes — полный размер файла на диске
//   LiveDataBytes  — суммарный размер записей, на которые ссылается индекс
//
// Разница (TotalFileBytes - LiveDataBytes) — это мёртвые данные:
// старые версии ключей и tombstone-записи.
//
//   SpaceAmplification = TotalFileBytes / LiveDataBytes
//
// Показывает, во сколько раз файл больше, чем живые данные.
// Например, если SpaceAmplification = 4.0, значит 75% файла — мусор.
//
//   ┌──────────────────────────────────────────────────────┐
//   │  Файл на диске (TotalFileBytes = 400 KB)             │
//   │                                                      │
//   │  ████████░░░░████░░░░░░░░████████████░░░░░░░░░░░░░░  │
//   │  ^^^^^^^^    ^^^^        ^^^^^^^^^^^^                │
//   │  живые       живые       живые                       │
//   │  данные      данные      данные                      │
//   │                                                      │
//   │  LiveDataBytes = 100 KB                              │
//   │  SpaceAmplification = 400 / 100 = 4.0                │
//   └──────────────────────────────────────────────────────┘
type Stats struct {
	LiveKeys       int
	TotalFileBytes int64
	LiveDataBytes  int64
	// SpaceAmplification = TotalFileBytes / LiveDataBytes
	// При SpaceAmplification > 3.0 — пора делать compaction
}

func (db *HashDB) Stats() (Stats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	info, err := db.log.file.Stat()
	if err != nil {
		return Stats{}, err
	}
	liveBytes := int64(0)
	for _, loc := range db.index {
		liveBytes += int64(loc.size)
	}
	return Stats{
		LiveKeys:       len(db.index),
		TotalFileBytes: info.Size(),
		LiveDataBytes:  liveBytes,
	}, nil
}
