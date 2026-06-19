package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// internal/storage/sst_test.go

// makeKeys возвращает n отсортированных ключей вида "key-00000001".
func makeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
	}
	return keys
}

func TestSSTWriteRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sst")

	// Write:
	w, _ := NewSSTWriter(path)
	keys := makeKeys(10_000) // отсортированные ключи
	for _, k := range keys {
		w.Add(k, []byte("value:"+string(k)))
	}
	w.Finish()

	// Read:
	r, _ := OpenSST(path)
	for _, k := range keys {
		val, err := r.Get(k)
		if err != nil {
			t.Fatalf("key %s: %v", k, err)
		}
		want := "value:" + string(k)
		if string(val) != want {
			t.Fatalf("key %s: got %q, want %q", k, val, want)
		}
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	bf := NewBloomFilter(100_000, 0.01)

	// Добавляем 100k ключей:
	for i := 0; i < 100_000; i++ {
		bf.Add(fmt.Appendf(nil, "key-%d", i))
	}

	// Проверяем 100k ключей которых точно нет:
	falsePositives := 0
	for i := 100_000; i < 200_000; i++ {
		if bf.MayContain(fmt.Appendf(nil, "key-%d", i)) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / 100_000.0
	if rate > 0.02 { // допускаем 2% (в 2 раза больше target)
		t.Fatalf("false positive rate too high: %.3f", rate)
	}
	t.Logf("false positive rate: %.4f", rate)
}

func TestSSTIterator(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iter.sst")

	// Пишем 1000 отсортированных ключей:
	w, err := NewSSTWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	const n = 1000
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%06d", i)
		val := fmt.Appendf(nil, "val-%d", i)
		if err := w.Add(key, val); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenSST(path)
	if err != nil {
		t.Fatal(err)
	}

	// Тест 1: полная итерация — все ключи в порядке.
	it := r.NewIterator()
	count := 0
	var prevKey []byte
	for ; it.Valid(); it.Next() {
		if prevKey != nil && bytes.Compare(it.Key(), prevKey) <= 0 {
			t.Fatalf("keys not sorted: %q <= %q", it.Key(), prevKey)
		}
		prevKey = append(prevKey[:0], it.Key()...)
		count++
	}
	if count != n {
		t.Fatalf("full scan: got %d keys, want %d", count, n)
	}

	// Тест 2: SeekGE на точное совпадение.
	it2 := r.NewIterator()
	it2.Seek([]byte("key-000500"))
	if !it2.Valid() {
		t.Fatal("SeekGE: iterator not valid")
	}
	if string(it2.Key()) != "key-000500" {
		t.Fatalf("SeekGE exact: got %q, want %q", it2.Key(), "key-000500")
	}

	// Тест 3: SeekGE между ключами.
	it3 := r.NewIterator()
	it3.Seek([]byte("key-000500x")) // между 500 и 501
	if !it3.Valid() {
		t.Fatal("SeekGE between: iterator not valid")
	}
	if string(it3.Key()) != "key-000501" {
		t.Fatalf("SeekGE between: got %q, want %q", it3.Key(), "key-000501")
	}

	// Тест 4: range scan [200, 300) — ровно 100 ключей.
	it4 := r.NewIterator()
	it4.Seek([]byte("key-000200"))
	rangeCount := 0
	for ; it4.Valid() && bytes.Compare(it4.Key(), []byte("key-000300")) < 0; it4.Next() {
		rangeCount++
	}
	if rangeCount != 100 {
		t.Fatalf("range scan [200,300): got %d, want 100", rangeCount)
	}

	// Тест 5: SeekGE за последним ключом → итератор невалиден.
	it5 := r.NewIterator()
	it5.Seek([]byte("zzz-beyond-all-keys"))
	if it5.Valid() {
		t.Fatalf("SeekGE past end: expected invalid iterator, got key %q", it5.Key())
	}

	// Тест 6: итерация с проверкой значений.
	it6 := r.NewIterator()
	it6.Seek([]byte("key-000010"))
	for i := 10; i < 15; i++ {
		if !it6.Valid() {
			t.Fatalf("iteration stopped early at i=%d", i)
		}
		wantKey := fmt.Sprintf("key-%06d", i)
		wantVal := fmt.Sprintf("val-%d", i)
		if string(it6.Key()) != wantKey {
			t.Fatalf("key mismatch: got %q, want %q", it6.Key(), wantKey)
		}
		if string(it6.Value()) != wantVal {
			t.Fatalf("val mismatch for key %s: got %q, want %q", wantKey, it6.Value(), wantVal)
		}
		it6.Next()
	}
}

func TestSSTLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test in short mode")
	}

	path := filepath.Join(t.TempDir(), "large.sst")

	const n = 1_000_000
	t.Logf("writing %d keys...", n)

	// Пишем 1M ключей:
	w, err := NewSSTWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%08d", i)
		val := fmt.Appendf(nil, "val-%d", i)
		if err := w.Add(key, val); err != nil {
			t.Fatalf("Add key %d: %v", i, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	// Проверяем размер файла:
	info, _ := os.Stat(path)
	t.Logf("SST file size: %.2f MB", float64(info.Size())/(1024*1024))

	r, err := OpenSST(path)
	if err != nil {
		t.Fatal(err)
	}

	// Читаем случайную выборку из 10k ключей:
	t.Logf("verifying random sample...")
	rng := rand.New(rand.NewSource(42))
	const sampleSize = 10_000
	for i := 0; i < sampleSize; i++ {
		idx := rng.Intn(n)
		key := fmt.Appendf(nil, "key-%08d", idx)
		wantVal := fmt.Sprintf("val-%d", idx)

		val, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get key %d: %v", idx, err)
		}
		if string(val) != wantVal {
			t.Fatalf("key %d: got %q, want %q", idx, val, wantVal)
		}
	}

	// Полный scan — проверяем количество и порядок:
	t.Logf("full scan...")
	it := r.NewIterator()
	count := 0
	var prevKey []byte
	for ; it.Valid(); it.Next() {
		if prevKey != nil && bytes.Compare(it.Key(), prevKey) <= 0 {
			t.Fatalf("keys not sorted at position %d", count)
		}
		prevKey = append(prevKey[:0], it.Key()...)
		count++
	}
	if count != n {
		t.Fatalf("full scan: got %d keys, want %d", count, n)
	}

	// Проверяем miss lookups (bloom filter должен отсечь):
	t.Logf("testing miss lookups (bloom filter)...")
	misses := 0
	for i := n; i < n+1000; i++ {
		key := fmt.Appendf(nil, "key-%08d", i) // ключи которых точно нет
		_, err := r.Get(key)
		if err == ErrNotFound {
			misses++
		}
	}
	if misses != 1000 {
		t.Fatalf("expected 1000 misses, got %d", misses)
	}
	t.Logf("all tests passed: 1M keys, random reads OK, full scan OK, misses OK")
}

func BenchmarkSSTGet(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.sst")

	const n = 100_000
	w, _ := NewSSTWriter(path)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Appendf(nil, "key-%08d", i)
		w.Add(keys[i], []byte("value"))
	}
	w.Finish()

	r, _ := OpenSST(path)

	b.Run("HitLookup", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.Get(keys[i%n])
		}
	})

	b.Run("MissLookup", func(b *testing.B) {
		missKey := fmt.Appendf(nil, "key-%08d", n+1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.Get(missKey)
		}
	})
}
