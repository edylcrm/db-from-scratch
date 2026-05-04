package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// internal/storage/wal_test.go

// checkVal — хелпер: проверяет значение ключа в SkipList.
func checkVal(t *testing.T, mem *SkipList, key, want string) {
	t.Helper()
	val, ok := mem.Get([]byte(key))
	if !ok || val == nil {
		t.Fatalf("key %q not found in memtable", key)
	}
	if string(val) != want {
		t.Fatalf("key %q: got %q, want %q", key, val, want)
	}
}

func TestWALRecovery(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	// Пишем данные:
	wal, _ := OpenWAL(walPath)
	wal.WritePut([]byte("a"), []byte("1"))
	wal.WritePut([]byte("b"), []byte("2"))
	wal.WriteDelete([]byte("a"))
	wal.WritePut([]byte("c"), []byte("3"))
	wal.file.Close()

	// Симулируем краш: дописываем мусор:
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte("incomplete record garbage"))
	f.Close()

	// Восстанавливаемся:
	wal2, _ := OpenWAL(walPath)
	mem := NewSkipList()
	if err := wal2.Recover(mem); err != nil {
		t.Fatal(err)
	}

	// "a" удалена (tombstone: value=nil, ok=true):
	if val, ok := mem.Get([]byte("a")); !ok || val != nil {
		t.Fatalf("expected 'a' to be tombstone (nil, true), got (%v, %v)", val, ok)
	}
	// "b" и "c" на месте:
	checkVal(t, mem, "b", "2")
	checkVal(t, mem, "c", "3")
}

func BenchmarkWALGroupCommit(b *testing.B) {
	const goroutines = 100

	b.Run("SingleFsync", func(b *testing.B) {
		walPath := filepath.Join(b.TempDir(), "wal-single")
		wal, err := OpenWAL(walPath)
		if err != nil {
			b.Fatal(err)
		}
		defer wal.file.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			key := []byte("benchkey")
			val := []byte("benchvalue-that-is-long-enough-to-be-realistic")
			for pb.Next() {
				if _, err := wal.WritePut(key, val); err != nil {
					b.Error(err)
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
	})

	b.Run("GroupCommit", func(b *testing.B) {
		walPath := filepath.Join(b.TempDir(), "wal-group")
		wal, err := NewGroupCommitWAL(walPath)
		if err != nil {
			b.Fatal(err)
		}
		defer wal.file.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			key := []byte("benchkey")
			val := []byte("benchvalue-that-is-long-enough-to-be-realistic")
			for pb.Next() {
				if _, err := wal.WritePut(key, val); err != nil {
					b.Error(err)
				}
			}
		})
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
	})
}

func TestWALGroupCommitCorrectness(t *testing.T) {
	// Проверяем что group commit не теряет данные при конкурентной записи.
	walPath := filepath.Join(t.TempDir(), "wal-gc")
	wal, err := NewGroupCommitWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	const (
		goroutines = 20
		writesEach = 500
	)

	var wg sync.WaitGroup
	var mu sync.Mutex
	written := make(map[string]string)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				key := fmt.Sprintf("g%d-k%d", id, i)
				val := fmt.Sprintf("g%d-v%d", id, i)
				if _, err := wal.WritePut([]byte(key), []byte(val)); err != nil {
					t.Errorf("WritePut: %v", err)
					return
				}
				mu.Lock()
				written[key] = val
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	wal.file.Close()

	// Восстанавливаем и проверяем что все записи на месте:
	wal2, _ := OpenWAL(walPath)
	mem := NewSkipList()
	if err := wal2.Recover(mem); err != nil {
		t.Fatal(err)
	}

	for key, wantVal := range written {
		gotVal, ok := mem.Get([]byte(key))
		if !ok || gotVal == nil {
			t.Errorf("key %q missing after recovery", key)
			continue
		}
		if string(gotVal) != wantVal {
			t.Errorf("key %q: got %q, want %q", key, gotVal, wantVal)
		}
	}
	t.Logf("recovered %d/%d writes correctly", len(written), goroutines*writesEach)
}

func TestWALTruncate(t *testing.T) {
	// Проверяем TruncateBefore: после truncate старые записи недоступны,
	// новые — на месте.
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := OpenWAL(walPath)

	// Пишем 10 записей и запоминаем LSN после 5-й:
	var lsnAfter5 LSN
	for i := 0; i < 10; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		lsn, err := wal.WritePut(key, []byte("val"))
		if err != nil {
			t.Fatal(err)
		}
		if i == 4 {
			lsnAfter5 = lsn
		}
	}

	// Truncate всё до lsnAfter5:
	if err := wal.TruncateBefore(lsnAfter5 + 1); err != nil {
		t.Fatal(err)
	}
	wal.file.Close()

	// Восстанавливаем — должны увидеть только записи с LSN > lsnAfter5:
	wal2, _ := OpenWAL(walPath)
	mem := NewSkipList()
	wal2.Recover(mem)

	// Записи 0-4 должны отсутствовать:
	for i := 0; i < 5; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		if _, ok := mem.Get(key); ok {
			t.Errorf("key-%d should have been truncated", i)
		}
	}

	// Записи 5-9 должны быть:
	for i := 5; i < 10; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		if _, ok := mem.Get(key); !ok {
			t.Errorf("key-%d missing after truncate", i)
		}
	}
}
