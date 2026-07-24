package storage

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// internal/storage/lsm_test.go

func TestLSMCorrectness(t *testing.T) {
	engine, _ := OpenLSM(t.TempDir())
	defer engine.Close()

	reference := make(map[string]string)
	rng := rand.New(rand.NewSource(42))

	const ops = 100_000
	for i := 0; i < ops; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(1000)) // намеренно мало уникальных ключей — много overwrite
		val := fmt.Sprintf("val-%d", rng.Intn(10000))

		op := rng.Intn(3)
		switch op {
		case 0, 1: // Set (66% операций)
			engine.Set([]byte(key), []byte(val))
			reference[key] = val
		case 2: // Delete (33% операций)
			engine.Delete([]byte(key))
			delete(reference, key)
		}

		// Каждые 1000 операций сверяемся с эталоном:
		if i%1000 == 0 {
			for k, want := range reference {
				got, err := engine.Get([]byte(k))
				if err != nil {
					t.Fatalf("op %d: key %s: %v", i, k, err)
				}
				if string(got) != want {
					t.Fatalf("op %d: key %s: got %q, want %q", i, k, got, want)
				}
			}
		}
	}
}

func TestLSMSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Пишем данные:
	e1, _ := OpenLSM(dir)
	e1.Set([]byte("persistent"), []byte("yes"))
	e1.Close()

	// Открываем заново:
	e2, _ := OpenLSM(dir)
	val, err := e2.Get([]byte("persistent"))
	if err != nil || string(val) != "yes" {
		t.Fatalf("got %q, %v", val, err)
	}
	e2.Close()
}

func TestLSMRaceConditions(t *testing.T) {
	// Запускай с: go test -race
	engine, _ := OpenLSM(t.TempDir())
	defer engine.Close()

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 10_000; i++ {
				key := fmt.Appendf(nil, "key-%d-%d", id, i)
				engine.Set(key, []byte("v"))
				engine.Get(key)
			}
		}(g)
	}
	wg.Wait()
}
