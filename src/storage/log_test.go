package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSetGet(t *testing.T) {
	db := openTemp(t)

	if err := db.Set([]byte("name"), []byte("alice")); err != nil {
		t.Fatal(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "alice" {
		t.Fatalf("got %q, want %q", val, "alice")
	}
}

func TestOverwrite(t *testing.T) {
	db := openTemp(t)
	db.Set([]byte("k"), []byte("v1"))
	db.Set([]byte("k"), []byte("v2"))

	val, _ := db.Get([]byte("k"))
	if string(val) != "v2" {
		t.Fatalf("expected v2, got %s", val)
	}
}

func TestDelete(t *testing.T) {
	db := openTemp(t)
	db.Set([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))

	_, err := db.Get([]byte("k"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.log")

	// Шаг 1: пишем несколько валидных записей и закрываем базу.
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db.Set([]byte("key1"), []byte("value1"))
	db.Set([]byte("key2"), []byte("value2"))
	db.Set([]byte("key3"), []byte("value3"))
	db.Close()

	// Шаг 2: симулируем краш — дописываем в конец файла мусор разной длины.
	// Это имитирует ситуацию когда процесс упал в середине записи:
	// может быть записан заголовок без данных, или данные без CRC, или вообще случайные байты.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Случай 1: оборванный заголовок (меньше 12 байт)
	f.Write([]byte{0xDE, 0xAD, 0xBE}) // только 3 байта вместо 12
	f.Close()

	// Шаг 3: открываем заново — должны увидеть все три валидных записи.
	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, err := db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("key1 missing after crash recovery: %v", err)
	}
	if string(val) != "value1" {
		t.Fatalf("key1: got %q, want %q", val, "value1")
	}

	val, err = db2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("key2 missing after crash recovery: %v", err)
	}
	if string(val) != "value2" {
		t.Fatalf("key2: got %q, want %q", val, "value2")
	}

	val, err = db2.Get([]byte("key3"))
	if err != nil {
		t.Fatalf("key3 missing after crash recovery: %v", err)
	}
	if string(val) != "value3" {
		t.Fatalf("key3: got %q, want %q", val, "value3")
	}
}

func TestCrashRecoveryMidRecord(t *testing.T) {
	// Тест 2: краш в середине данных записи (заголовок есть, данные оборваны).
	dir := t.TempDir()
	path := filepath.Join(dir, "crash2.log")

	db, _ := Open(path)
	db.Set([]byte("before"), []byte("crash"))
	db.Close()

	// Дописываем полный заголовок (12 байт) но без данных:
	// CRC=0, key_len=10, val_len=10 — данных не будет
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header[0:4], 0xDEADBEEF) // неверный CRC
	binary.LittleEndian.PutUint32(header[4:8], 10)         // key_len=10
	binary.LittleEndian.PutUint32(header[8:12], 10)        // val_len=10
	f.Write(header)
	f.Write([]byte("only5")) // только 5 байт вместо 20
	f.Close()

	db2, _ := Open(path)
	defer db2.Close()

	val, err := db2.Get([]byte("before"))
	if err != nil {
		t.Fatalf("valid record lost after crash: %v", err)
	}
	if string(val) != "crash" {
		t.Fatalf("got %q, want %q", val, "crash")
	}
}

func TestCrashRecoveryBadCRC(t *testing.T) {
	// Тест 3: запись с верной длиной, но неверным CRC (bit flip на диске).
	dir := t.TempDir()
	path := filepath.Join(dir, "crash3.log")

	db, _ := Open(path)
	db.Set([]byte("good"), []byte("data"))
	db.Close()

	// Читаем файл и портим CRC последней записи:
	data, _ := os.ReadFile(path)
	data[0] ^= 0xFF // инвертируем первый байт CRC
	os.WriteFile(path, data, 0644)

	// Открываем — запись с плохим CRC должна быть отброшена.
	// Файл состоит из одной записи, поэтому база будет пустой — это корректно.
	db2, _ := Open(path)
	defer db2.Close()

	_, err := db2.Get([]byte("good"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for corrupted record, got %v", err)
	}
}

func BenchmarkGet(b *testing.B) {
	db := openTemp(b)

	// Наполняем 10k ключей:
	const n = 10_000
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%06d", i)
		db.Set(key, []byte("value"))
	}

	// Benchmark Get — должен расти линейно с n:
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Appendf(nil, "key-%06d", i%n)
		db.Get(key)
	}
}

// openTemp принимает testing.TB — работает и для тестов и для бенчмарков.
func openTemp(tb testing.TB) *Log {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "test.log")
	db, err := Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	return db
}
