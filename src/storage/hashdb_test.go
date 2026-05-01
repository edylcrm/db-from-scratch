package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestHashDBSetGet(t *testing.T) {
	db, _ := OpenHashDB(filepath.Join(t.TempDir(), "db.log"))

	db.Set([]byte("name"), []byte("alice"))

	val, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "alice" {
		t.Fatalf("got %q, want alice", val)
	}
}

func TestHashDBGetMissing(t *testing.T) {
	db, _ := OpenHashDB(filepath.Join(t.TempDir(), "db.log"))
	_, err := db.Get([]byte("nonexistent"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestHashDBOverwrite(t *testing.T) {
	db, _ := OpenHashDB(filepath.Join(t.TempDir(), "db.log"))
	db.Set([]byte("k"), []byte("v1"))
	db.Set([]byte("k"), []byte("v2"))
	db.Set([]byte("k"), []byte("v3"))

	val, _ := db.Get([]byte("k"))
	if string(val) != "v3" {
		t.Fatalf("got %q, want v3", val)
	}
}

func TestHashDBDelete(t *testing.T) {
	db, _ := OpenHashDB(filepath.Join(t.TempDir(), "db.log"))
	db.Set([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))

	_, err := db.Get([]byte("k"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestHashDBReopenPreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.log")

	db, _ := OpenHashDB(path)
	db.Set([]byte("hello"), []byte("world"))
	db.Close()

	db2, _ := OpenHashDB(path)
	defer db2.Close()

	val, err := db2.Get([]byte("hello"))
	if err != nil || string(val) != "world" {
		t.Fatalf("got %q, %v; want world", val, err)
	}
}

func TestHashDBSpaceAmplification(t *testing.T) {
	db, _ := OpenHashDB(filepath.Join(t.TempDir(), "db.log"))

	// После 10k overwrite одного ключа space amplification должна быть > 2.
	const writes = 10_000
	for i := 0; i < writes; i++ {
		db.Set([]byte("hotkey"), fmt.Appendf(nil, "value-%d", i))
	}

	stats, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	amp := float64(stats.TotalFileBytes) / float64(stats.LiveDataBytes)
	t.Logf("SpaceAmplification = %.1fx", amp)
	if amp < 2.0 {
		t.Fatalf("expected amp > 2.0, got %.1f", amp)
	}
}

func TestHashDBStatsLiveKeys(t *testing.T) {
	db, _ := OpenHashDB(filepath.Join(t.TempDir(), "db.log"))
	for i := 0; i < 10; i++ {
		db.Set(fmt.Appendf(nil, "k%d", i), []byte("v"))
	}
	db.Delete([]byte("k0"))
	db.Delete([]byte("k1"))

	stats, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.LiveKeys != 8 {
		t.Fatalf("expected 8 live keys, got %d", stats.LiveKeys)
	}
}

func TestBuildIndexSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db.log")

	// Пишем данные:
	db, _ := OpenHashDB(path)
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("a"), []byte("3")) // overwrite
	db.Close()

	// "Краш": дописываем мусор в конец файла
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte("corrupted garbage that is not a valid record"))
	f.Close()

	// Открываем заново — должны увидеть валидные данные
	db2, err := OpenHashDB(path)
	if err != nil {
		t.Fatal(err)
	}

	val, _ := db2.Get([]byte("a"))
	if string(val) != "3" {
		t.Fatalf("want 3, got %s", val)
	}
	val, _ = db2.Get([]byte("b"))
	if string(val) != "2" {
		t.Fatalf("want 2, got %s", val)
	}
}

func TestHashDBGetDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.log")

	db, _ := OpenHashDB(path)
	db.Set([]byte("secret"), []byte("data"))
	db.Close()

	// Портим данные: перезаписываем несколько байт в середине файла.
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	f.WriteAt([]byte("XXXX"), 10) // попадаем в payload — CRC не совпадёт
	f.Close()

	// Открываем заново — buildIndex пропустит битую запись.
	db2, err := OpenHashDB(path)
	if err != nil {
		t.Fatal(err)
	}

	// Ключ не попал в индекс (запись битая) — должен быть ErrNotFound.
	_, err = db2.Get([]byte("secret"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for corrupted record, got %v", err)
	}
	db2.Close()
}

func TestHashDBGetCRCCheckOnRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.log")

	db, _ := OpenHashDB(path)
	db.Set([]byte("key"), []byte("value"))

	// Портим данные после построения индекса (индекс уже указывает на запись).
	// Перезаписываем value-часть записи, не трогая заголовок.
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	// recordHeaderSize(12) + len("key")(3) = 15 — начало value
	f.WriteAt([]byte("XXXXX"), 15)
	f.Close()

	// Get должен обнаружить CRC mismatch.
	_, err := db.Get([]byte("key"))
	if err != ErrCorrupted {
		t.Fatalf("expected ErrCorrupted, got %v", err)
	}
	db.Close()
}

func BenchmarkHashDBGet(b *testing.B) {
	db, _ := OpenHashDB(filepath.Join(b.TempDir(), "bench.log"))
	// Наполняем 100k ключей:
	for i := 0; i < 100_000; i++ {
		key := fmt.Appendf(nil, "key-%d", i)
		db.Set(key, []byte("value"))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Appendf(nil, "key-%d", i%100_000)
			db.Get(key)
			i++
		}
	})
}
