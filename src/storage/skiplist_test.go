package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestSkipListOrder(t *testing.T) {
	sl := NewSkipList()
	keys := []string{"banana", "apple", "cherry", "avocado", "blueberry"}
	for _, k := range keys {
		sl.Set([]byte(k), []byte("v:"+k))
	}

	// Итерация должна дать ключи в отсортированном порядке:
	it := sl.NewIterator()
	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}

	want := []string{"apple", "avocado", "banana", "blueberry", "cherry"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSkipListPropertyBased(t *testing.T) {
	// Property-based тест без внешних зависимостей: генерируем случайные операции
	// и сверяем SkipList с эталонной map на каждом шаге.
	rng := rand.New(rand.NewSource(42))

	const (
		iterations = 5000
		keySpace   = 200
	)

	sl := NewSkipList()
	ref := make(map[string]string)

	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("k%03d", rng.Intn(keySpace))
		val := fmt.Sprintf("v%d", rng.Intn(1000))

		op := rng.Intn(3)
		switch op {
		case 0:
			sl.Set([]byte(key), []byte(val))
			ref[key] = val

		case 1:
			sl.Delete([]byte(key))
			delete(ref, key)

		case 2:
			slVal, slOK := sl.Get([]byte(key))
			refVal, refOK := ref[key]

			slActualOK := slOK && slVal != nil

			if slActualOK != refOK {
				t.Fatalf("op %d: Get(%q): skiplist.ok=%v, map.ok=%v",
					i, key, slActualOK, refOK)
			}
			if refOK && string(slVal) != refVal {
				t.Fatalf("op %d: Get(%q): skiplist=%q, map=%q",
					i, key, slVal, refVal)
			}
		}
	}

	it := sl.NewIterator()
	var slKeys []string
	for ; it.Valid(); it.Next() {
		if it.Value() != nil {
			k := string(it.Key())
			slVal := string(it.Value())
			refVal, ok := ref[k]
			if !ok {
				t.Fatalf("skiplist has key %q not in reference map", k)
			}
			if slVal != refVal {
				t.Fatalf("key %q: skiplist=%q, map=%q", k, slVal, refVal)
			}
			slKeys = append(slKeys, k)
		}
	}

	if len(slKeys) != len(ref) {
		t.Fatalf("skiplist has %d keys, map has %d keys", len(slKeys), len(ref))
	}

	for i := 1; i < len(slKeys); i++ {
		if slKeys[i] <= slKeys[i-1] {
			t.Fatalf("keys not sorted at position %d: %q <= %q",
				i, slKeys[i], slKeys[i-1])
		}
	}
}

func TestSkipListConcurrent(t *testing.T) {
	// Проверяем что SkipList не ломается при concurrent ЗАПИСЯХ.
	// Этот тест НЕ ловит гонки между чтением и записью —
	// для этого см. TestSkipListConcurrentReadWrite.
	sl := NewSkipList()
	var wg sync.WaitGroup

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := fmt.Appendf(nil, "writer-%d-key-%d", id, i)
				sl.Set(key, []byte("val"))
			}
		}(g)
	}

	wg.Wait()

	for g := 0; g < 10; g++ {
		for i := 0; i < 1000; i++ {
			key := fmt.Appendf(nil, "writer-%d-key-%d", g, i)
			if _, ok := sl.Get(key); !ok {
				t.Fatalf("key %s missing", key)
			}
		}
	}
}

func TestSkipListSeekExactMatch(t *testing.T) {
	sl := NewSkipList()
	for _, k := range []string{"apple", "banana", "cherry"} {
		sl.Set([]byte(k), []byte("v:"+k))
	}

	it := sl.NewIterator()
	it.Seek([]byte("banana"))

	if !it.Valid() {
		t.Fatal("Seek(banana): expected valid iterator")
	}
	if got := string(it.Key()); got != "banana" {
		t.Fatalf("Seek(banana): got key %q, want %q", got, "banana")
	}
}

func TestSkipListSeekBetweenKeys(t *testing.T) {
	sl := NewSkipList()
	for _, k := range []string{"apple", "cherry"} {
		sl.Set([]byte(k), []byte("v"))
	}

	it := sl.NewIterator()
	it.Seek([]byte("banana")) // между apple и cherry

	if !it.Valid() {
		t.Fatal("Seek(banana): expected valid iterator")
	}
	if got := string(it.Key()); got != "cherry" {
		t.Fatalf("Seek(banana) between apple/cherry: got %q, want %q", got, "cherry")
	}
}

func TestSkipListSeekBeforeAllKeys(t *testing.T) {
	sl := NewSkipList()
	for _, k := range []string{"banana", "cherry"} {
		sl.Set([]byte(k), []byte("v"))
	}

	it := sl.NewIterator()
	it.Seek([]byte("apple"))

	if !it.Valid() {
		t.Fatal("Seek(apple): expected valid iterator")
	}
	if got := string(it.Key()); got != "banana" {
		t.Fatalf("Seek(apple): got %q, want %q", got, "banana")
	}
}

func TestSkipListSeekAfterAllKeys(t *testing.T) {
	sl := NewSkipList()
	for _, k := range []string{"apple", "banana"} {
		sl.Set([]byte(k), []byte("v"))
	}

	it := sl.NewIterator()
	it.Seek([]byte("zebra"))

	if it.Valid() {
		t.Fatalf("Seek past end: expected invalid iterator, got key %q", it.Key())
	}
}

func TestSkipListSeekOnEmpty(t *testing.T) {
	sl := NewSkipList()
	it := sl.NewIterator()
	it.Seek([]byte("anything"))

	if it.Valid() {
		t.Fatal("Seek on empty: expected invalid iterator")
	}
}

func TestSkipListRangeScanContents(t *testing.T) {
	sl := NewSkipList()
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	for _, k := range keys {
		sl.Set([]byte(k), []byte("v:"+k))
	}

	tests := []struct {
		name       string
		start, end string
		want       []string
	}{
		{"exact-range", "c", "f", []string{"c", "d", "e"}},
		{"start-at-first", "a", "c", []string{"a", "b"}},
		{"end-at-last", "e", "g", []string{"e", "f"}},
		{"between-keys", "bb", "dd", []string{"c", "d"}},
		{"before-all", "0", "b", []string{"a"}},
		{"after-all", "h", "z", []string{}},
		{"empty-range", "c", "c", []string{}},
		{"full-range", "", "zz", keys},
		{"single-key", "c", "d", []string{"c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvs := sl.RangeScan([]byte(tt.start), []byte(tt.end))

			got := make([]string, len(kvs))
			for i, kv := range kvs {
				got[i] = string(kv.Key)
			}

			if len(got) != len(tt.want) {
				t.Fatalf("RangeScan(%q, %q): got %v (len=%d), want %v (len=%d)",
					tt.start, tt.end, got, len(got), tt.want, len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("RangeScan(%q, %q) at pos %d: got %q, want %q",
						tt.start, tt.end, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSkipListRangeScanVsSortedMap(t *testing.T) {
	// Property-based: RangeScan должен возвращать то же что map + sort,
	// на множестве случайных диапазонов.
	rng := rand.New(rand.NewSource(42))
	sl := NewSkipList()
	ref := make(map[string]string)

	const n = 500
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%04d", rng.Intn(2000))
		val := fmt.Sprintf("v%d", i)
		sl.Set([]byte(key), []byte(val))
		ref[key] = val
	}

	for trial := 0; trial < 50; trial++ {
		start := fmt.Sprintf("k%04d", rng.Intn(2000))
		end := fmt.Sprintf("k%04d", rng.Intn(2000))
		if start > end {
			start, end = end, start
		}

		// Эталон: собираем из map и сортируем.
		var want []string
		for k := range ref {
			if k >= start && k < end {
				want = append(want, k)
			}
		}
		sort.Strings(want)

		got := sl.RangeScan([]byte(start), []byte(end))
		gotKeys := make([]string, len(got))
		for i, kv := range got {
			gotKeys[i] = string(kv.Key)
		}

		if len(gotKeys) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(gotKeys, want) {
			t.Fatalf("RangeScan(%q, %q):\n  got:  %v\n  want: %v",
				start, end, gotKeys, want)
		}
	}
}

func TestSkipListByteSizeAfterInsert(t *testing.T) {
	sl := NewSkipList()

	sl.Set([]byte("abc"), []byte("xxxxx")) // 3 + 5 = 8
	if sl.byteSize != 8 {
		t.Fatalf("after first insert: byteSize=%d, want 8", sl.byteSize)
	}

	sl.Set([]byte("de"), []byte("yyy")) // +2+3 = 13
	if sl.byteSize != 13 {
		t.Fatalf("after second insert: byteSize=%d, want 13", sl.byteSize)
	}
}

func TestSkipListByteSizeAfterOverwrite(t *testing.T) {
	sl := NewSkipList()

	sl.Set([]byte("abc"), []byte("xxxxx"))
	if sl.byteSize != 8 {
		t.Fatalf("initial: byteSize=%d, want 8", sl.byteSize)
	}

	// Overwrite c меньшим value: 3 + 2 = 5
	sl.Set([]byte("abc"), []byte("yy"))
	if sl.byteSize != 5 {
		t.Fatalf("after shorter overwrite: byteSize=%d, want 5", sl.byteSize)
	}

	// Overwrite c большим value: 3 + 10 = 13
	sl.Set([]byte("abc"), []byte("0123456789"))
	if sl.byteSize != 13 {
		t.Fatalf("after longer overwrite: byteSize=%d, want 13", sl.byteSize)
	}

	// Overwrite тем же размером — byteSize не меняется
	before := sl.byteSize
	sl.Set([]byte("abc"), []byte("abcdefghij"))
	if sl.byteSize != before {
		t.Fatalf("after same-size overwrite: byteSize=%d, want %d",
			sl.byteSize, before)
	}
}

func TestSkipListByteSizeAfterDelete(t *testing.T) {
	sl := NewSkipList()

	sl.Set([]byte("abc"), []byte("xxxxx")) // byteSize=8
	sl.Delete([]byte("abc"))               // Set(key, nil)

	// После Delete остаётся только ключ (tombstone-запись в памяти).
	if sl.byteSize != 3 {
		t.Fatalf("after delete: byteSize=%d, want 3 (key only)", sl.byteSize)
	}
}

func TestSkipListByteSizeMultipleOperations(t *testing.T) {
	sl := NewSkipList()

	sl.Set([]byte("k1"), []byte("val1")) // 2+4 = 6
	sl.Set([]byte("k2"), []byte("val2")) // +2+4 → 12
	sl.Set([]byte("k1"), []byte("V"))    // 6 - 4 + 1 → 9
	sl.Delete([]byte("k2"))              // 9 - 4 → 5

	if sl.byteSize != 5 {
		t.Fatalf("byteSize=%d, want 5 (k1=2+1, k2=2+0)", sl.byteSize)
	}
}

func TestSkipListValueImmutableAfterInsert(t *testing.T) {
	sl := NewSkipList()

	value := []byte("original")
	sl.Set([]byte("k"), value)

	// Мутируем свой буфер после Set:
	value[0] = 'X'
	value[1] = 'Y'

	got, ok := sl.Get([]byte("k"))
	if !ok {
		t.Fatal("key not found")
	}
	if string(got) != "original" {
		t.Fatalf("stored value was mutated by caller: got %q, want %q",
			got, "original")
	}
}

func TestSkipListValueImmutableAfterOverwrite(t *testing.T) {
	// Главный тест — ловит баг "value не копируется на overwrite-ветке".
	sl := NewSkipList()
	sl.Set([]byte("k"), []byte("first"))

	overwrite := []byte("second")
	sl.Set([]byte("k"), overwrite)

	// Мутируем буфер которым перезаписывали:
	overwrite[0] = 'X'
	overwrite[1] = 'Y'

	got, ok := sl.Get([]byte("k"))
	if !ok {
		t.Fatal("key not found")
	}
	if string(got) != "second" {
		t.Fatalf("value mutated after overwrite: got %q, want %q",
			got, "second")
	}
}

func TestSkipListKeyImmutable(t *testing.T) {
	sl := NewSkipList()

	key := []byte("original-key")
	sl.Set(key, []byte("v"))

	// Мутируем буфер ключа после вставки:
	key[0] = 'X'

	// Ищем по оригинальному значению:
	if _, ok := sl.Get([]byte("original-key")); !ok {
		t.Fatal("caller's key mutation corrupted stored key")
	}
}

func TestSkipListTombstoneGet(t *testing.T) {
	// После Delete ключ остаётся в skiplist как tombstone.
	// Get возвращает (nil, true) — это сигнал LSM-движку "ключ точно
	// удалён в этом memtable, не искать в нижних уровнях".
	sl := NewSkipList()
	sl.Set([]byte("k"), []byte("v"))
	sl.Delete([]byte("k"))

	val, ok := sl.Get([]byte("k"))
	if !ok {
		t.Fatal("Get(deleted): want ok=true (tombstone is present), got ok=false")
	}
	if val != nil {
		t.Fatalf("Get(deleted): want nil value, got %q", val)
	}
}

func TestSkipListGetMissing(t *testing.T) {
	sl := NewSkipList()
	sl.Set([]byte("other"), []byte("v"))

	val, ok := sl.Get([]byte("missing"))
	if ok {
		t.Fatalf("Get(missing): want ok=false, got ok=true val=%q", val)
	}
	if val != nil {
		t.Fatalf("Get(missing): want nil value, got %q", val)
	}
}

func TestSkipListTombstoneThenSet(t *testing.T) {
	// После Delete повторный Set должен "воскресить" ключ:
	// Get возвращает (value, true) с нормальным значением.
	sl := NewSkipList()
	sl.Set([]byte("k"), []byte("v1"))
	sl.Delete([]byte("k"))
	sl.Set([]byte("k"), []byte("v2"))

	val, ok := sl.Get([]byte("k"))
	if !ok || string(val) != "v2" {
		t.Fatalf("after resurrect: got val=%q ok=%v, want v2/true", val, ok)
	}
}

func TestSkipListLevelDistribution(t *testing.T) {
	// В правильном skiplist на верхних уровнях должно быть существенно
	// меньше узлов чем на нижних (1/branchFactor на каждый уровень вверх).
	sl := NewSkipList()
	const n = 10_000
	for i := 0; i < n; i++ {
		sl.Set(fmt.Appendf(nil, "k%06d", i), []byte("v"))
	}

	// Считаем узлы на каждом уровне:
	counts := make([]int, sl.height)
	for i := 0; i < sl.height; i++ {
		curr := sl.head.next[i]
		for curr != nil {
			counts[i]++
			curr = curr.next[i]
		}
	}

	// Уровень 0 содержит все узлы:
	if counts[0] != n {
		t.Fatalf("level 0: got %d nodes, want %d", counts[0], n)
	}

	// Каждый следующий уровень должен быть существенно меньше.
	// При branchFactor=4 ожидаем ratio ~0.25. Допускаем большой разброс,
	// но каждый верхний уровень должен быть <= 60% от нижнего.
	for i := 1; i < sl.height; i++ {
		if counts[i] == 0 {
			break // достигли пустых верхних уровней — норм
		}
		ratio := float64(counts[i]) / float64(counts[i-1])
		if ratio > 0.6 {
			t.Fatalf("level %d has %d nodes (%.1f%% of level %d=%d); "+
				"expected ~25%% — skiplist degenerated to linked list?",
				i, counts[i], ratio*100, i-1, counts[i-1])
		}
	}
	t.Logf("level distribution: %v", counts)
}

func TestSkipListConcurrentReadWrite(t *testing.T) {
	sl := NewSkipList()

	// Предзаполняем — чтобы RangeScan имел что обходить:
	for i := 0; i < 100; i++ {
		sl.Set(fmt.Appendf(nil, "k%04d", i), []byte("v"))
	}

	var wg sync.WaitGroup

	// Писатели:
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				sl.Set(fmt.Appendf(nil, "writer-%d-%d", id, i), []byte("v"))
			}
		}(w)
	}

	// Читатели через RangeScan (проверяют лок в RangeScan):
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_ = sl.RangeScan([]byte("k0000"), []byte("z"))
			}
		}()
	}

	// Читатели через Get (у Get лок есть, это для общего нагруза):
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				sl.Get(fmt.Appendf(nil, "k%04d", i%100))
			}
		}()
	}

	// Читатели через прямой итератор (проверяют безопасность NewIterator/Next):
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				it := sl.NewIterator()
				for j := 0; it.Valid() && j < 50; j++ {
					_ = it.Key()
					_ = it.Value()
					it.Next()
				}
			}
		}()
	}

	wg.Wait()
	// Тест "проходит" если `go test -race` не ругается.
}

func BenchmarkSkipListVsMap(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}

	for _, n := range sizes {
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = fmt.Appendf(nil, "key-%08d", i)
		}

		b.Run(fmt.Sprintf("SkipList/Set/n=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sl := NewSkipList()
				for j := 0; j < n; j++ {
					sl.Set(keys[j], []byte("v"))
				}
			}
		})

		b.Run(fmt.Sprintf("Map/Set/n=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := make(map[string][]byte, n)
				for j := 0; j < n; j++ {
					m[string(keys[j])] = []byte("v")
				}
			}
		})

		sl := NewSkipList()
		m := make(map[string][]byte, n)
		for j := 0; j < n; j++ {
			sl.Set(keys[j], []byte("v"))
			m[string(keys[j])] = []byte("v")
		}

		b.Run(fmt.Sprintf("SkipList/Get/n=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sl.Get(keys[i%n])
			}
		})

		b.Run(fmt.Sprintf("Map/Get/n=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = m[string(keys[i%n])]
			}
		})

		scanStart := keys[n/4]
		scanEnd := keys[n/2]

		b.Run(fmt.Sprintf("SkipList/RangeScan/n=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sl.RangeScan(scanStart, scanEnd)
			}
		})

		b.Run(fmt.Sprintf("Map/RangeScan/n=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var result [][]byte
				for k := range m {
					kb := []byte(k)
					if bytes.Compare(kb, scanStart) >= 0 && bytes.Compare(kb, scanEnd) < 0 {
						result = append(result, kb)
					}
				}
				sort.Slice(result, func(i, j int) bool {
					return bytes.Compare(result[i], result[j]) < 0
				})
			}
		})
	}
}
