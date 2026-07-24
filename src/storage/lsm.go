// internal/storage/lsm.go

package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

type LSMEngine struct {
	mu  sync.RWMutex
	dir string

	// Memory layer:
	memTable   *SkipList   // Активная, принимает новые записи
	immutables []*SkipList // Ожидают flush (от новых к старым)

	// Disk layer (levels[0] = L0, levels[1] = L1, ...):
	levels [][]*SSTReader

	// WAL для активной MemTable:
	wal     *WAL
	walPath string
	// Номера WAL-файлов: [0]=oldest immutable WAL, ..., last=активный WAL.
	// При flush самый старый удаляется; при ротации добавляется новый.
	walNums []uint64

	// Монотонный счётчик для именования WAL и SST файлов.
	// Единый для всех типов файлов (как в LevelDB).
	nextFileNum atomic.Uint64

	// Порог в байтах после которого MemTable → immutable:
	memTableMaxBytes int64
	// Сигналы для фоновых горутин:
	flushCh chan struct{}

	compactCh      chan int            // Номер уровня для compaction
	compactPointer map[int]uint64     // Round-robin индекс для каждого уровня
	closed    atomic.Bool
	// bgErr хранит ошибку фонового процесса (flush/compact).
	// Если не nil — engine в read-only режиме, Set/Delete возвращают ошибку.
	bgErr atomic.Pointer[error]
	wg    sync.WaitGroup
}

func OpenLSM(dir string) (*LSMEngine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	e := &LSMEngine{
		dir:              dir,
		memTable:         NewSkipList(),
		levels:           make([][]*SSTReader, 1), // L0 всегда существует
		memTableMaxBytes: 64 * 1024 * 1024,        // 64MB
		flushCh:          make(chan struct{}, 1),
		compactCh:        make(chan int, 4),
		compactPointer:   make(map[int]uint64),
	}

	// Загружаем manifest чтобы узнать какие SST файлы существуют:
	manifest, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}

	// Открываем существующие SST файлы:
	for _, levelPaths := range manifest.Levels {
		var level []*SSTReader
		for _, path := range levelPaths {
			sst, err := OpenSST(path)
			if err != nil {
				return nil, fmt.Errorf("open SST %s: %w", path, err)
			}
			level = append(level, sst)
		}
		e.levels = append(e.levels, level)
	}

	// Восстанавливаем nextFileNum из manifest:
	e.nextFileNum.Store(manifest.NextFileNum)

	// WAL recovery: replay все WAL файлы, перечисленные в manifest.
	// Каждый WAL соответствует одной memtable (активная или immutable).
	// Порядок: от старых к новым — старые идут в immutables, последний становится активным.
	walNums := manifest.WALFileNums
	if len(walNums) == 0 {
		// Первый запуск или миграция — создаём новый WAL.
		num := e.allocFileNum()
		walNums = []uint64{num}
	}

	for i, num := range walNums {
		wp := walPathForNum(dir, num)
		w, err := OpenWAL(wp)
		if err != nil {
			return nil, fmt.Errorf("open WAL %s: %w", wp, err)
		}

		mem := NewSkipList()
		if err := w.Recover(mem); err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("recover WAL %s: %w", wp, err)
		}

		if i < len(walNums)-1 {
			// Старые WAL — их memtables идут в immutables (от новых к старым).
			_ = w.Close()
			e.immutables = append([]*SkipList{mem}, e.immutables...)
		} else {
			// Последний WAL — активный.
			e.wal = w
			e.walPath = wp
			e.memTable = mem
		}
	}
	e.walNums = walNums

	// Сохраняем MANIFEST сразу при первом запуске, до первой записи.
	// Без этого крэш до первого flush потеряет данные из WAL,
	// потому что при recovery не будет MANIFEST с номерами WAL файлов.
	if err := e.saveManifest(); err != nil {
		return nil, err
	}

	e.wg.Add(2)
	go e.flushWorker()
	go e.compactWorker()

	return e, nil
}

func (e *LSMEngine) Set(key, value []byte) error {
	if err := e.bgErr.Load(); err != nil {
		return fmt.Errorf("engine is read-only due to background error: %w", *err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := e.wal.WritePut(key, value); err != nil {
		return err
	}

	e.memTable.Set(key, value)

	if e.memTable.byteSize >= e.memTableMaxBytes {
		e.immutables = append([]*SkipList{e.memTable}, e.immutables...)
		e.memTable = NewSkipList()

		// Открываем новый WAL для новой MemTable:
		num := e.allocFileNum()
		newWALPath := walPathForNum(e.dir, num)
		newWAL, err := OpenWAL(newWALPath)
		if err != nil {
			return err
		}
		e.wal = newWAL
		e.walPath = newWALPath
		e.walNums = append(e.walNums, num)

		// Сигнал flush горутине (non-blocking):
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (e *LSMEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Поиск от свежих данных к старым:

	// 1. Активная MemTable:
	if v, ok := e.memTable.Get(key); ok {
		return valueOrNotFound(v)
	}

	// 2. Immutable MemTables (от свежих к старым):
	// immutables[0] = самая свежая, immutables[len-1] = самая старая.
	for _, imm := range e.immutables {
		if v, ok := imm.Get(key); ok {
			return valueOrNotFound(v)
		}
	}

	// 3. L0 — может иметь overlap между файлами, смотрим все от свежих к старым:
	if len(e.levels) > 0 {
		for i := len(e.levels[0]) - 1; i >= 0; i-- {
			if v, err := e.levels[0][i].Get(key); err == nil {
				return v, nil
			}
		}
	}

	// 4. L1+ — нет overlap, бинарный поиск нужного файла по ключевому диапазону:
	for level := 1; level < len(e.levels); level++ {
		files := e.levels[level]
		// Бинарный поиск: ищем первый файл, чей MaxKey >= key
		lo, hi := 0, len(files)-1
		idx := -1
		for lo <= hi {
			mid := (lo + hi) / 2
			if bytes.Compare(files[mid].MaxKey(), key) < 0 {
				lo = mid + 1
			} else {
				idx = mid
				hi = mid - 1
			}
		}
		if idx == -1 {
			continue // ключ больше всех файлов на этом уровне
		}
		// Проверяем что ключ попадает в диапазон файла:
		if bytes.Compare(files[idx].MinKey(), key) <= 0 {
			if v, err := files[idx].Get(key); err == nil {
				return v, nil
			}
		}
	}

	return nil, ErrNotFound
}

func (e *LSMEngine) Delete(key []byte) error {
	if err := e.bgErr.Load(); err != nil {
		return fmt.Errorf("engine is read-only due to background error: %w", *err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := e.wal.WriteDelete(key); err != nil {
		return err
	}

	e.memTable.Delete(key)

	// Проверка на переполнение — аналогично Set:
	if e.memTable.byteSize >= e.memTableMaxBytes {
		e.immutables = append([]*SkipList{e.memTable}, e.immutables...)
		e.memTable = NewSkipList()

		num := e.allocFileNum()
		newWALPath := walPathForNum(e.dir, num)
		newWAL, err := OpenWAL(newWALPath)
		if err != nil {
			return err
		}
		e.wal = newWAL
		e.walPath = newWALPath
		e.walNums = append(e.walNums, num)

		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (e *LSMEngine) Close() error {
	e.closed.Store(true)

	// Закрываем каналы — горутины завершатся:
	close(e.flushCh)
	close(e.compactCh)

	// Ждём завершения фоновых горутин:
	e.wg.Wait()

	// Закрываем WAL:
	if err := e.wal.Close(); err != nil {
		return err
	}

	// Закрываем все SST readers:
	for _, level := range e.levels {
		for _, sst := range level {
			_ = sst.Close()
		}
	}

	return nil
}

// flushWorker — фоновая горутина, сбрасывает immutable MemTable в SST файл.
func (e *LSMEngine) flushWorker() {
	defer e.wg.Done()
	for range e.flushCh {
		if e.closed.Load() {
			return
		}
		if err := e.flushOldestImmutable(); err != nil {
			// Переводим engine в read-only режим. Set/Delete будут возвращать ошибку,
			// но Get продолжит работать. Это лучше, чем panic — existing connections
			// могут дочитать данные.
			e.bgErr.Store(&err)
			return
		}
	}
}

func (e *LSMEngine) compactWorker() {
	defer e.wg.Done()
	for level := range e.compactCh {
		if e.closed.Load() {
			return
		}
		if err := e.compactLevel(level); err != nil {
			e.bgErr.Store(&err)
			return
		}
	}
}

func (e *LSMEngine) flushOldestImmutable() error {
	e.mu.Lock()
	if len(e.immutables) == 0 {
		e.mu.Unlock()
		return nil
	}
	// Берём самую старую (последняя в slice):
	mem := e.immutables[len(e.immutables)-1]
	e.mu.Unlock()

	num := e.allocFileNum()
	sstPath := filepath.Join(e.dir, fmt.Sprintf("%06d-l0.sst", num))
	sstWriter, err := NewSSTWriter(sstPath)
	if err != nil {
		return err
	}

	it := mem.NewIterator()
	for ; it.Valid(); it.Next() {
		if err := sstWriter.Add(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	if err := sstWriter.Finish(); err != nil {
		return err
	}

	sstReader, err := OpenSST(sstPath)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.levels[0] = append(e.levels[0], sstReader)
	e.immutables = e.immutables[:len(e.immutables)-1]

	// Удаляем самый старый WAL (соответствует flush'нутой immutable).
	// walNums[0] = oldest immutable, walNums[last] = активный WAL.
	var oldWALPath string
	if len(e.walNums) > 1 {
		oldWALPath = walPathForNum(e.dir, e.walNums[0])
		e.walNums = e.walNums[1:]
	}
	e.mu.Unlock()

	if oldWALPath != "" {
		_ = os.Remove(oldWALPath)
	}

	if err := e.saveManifest(); err != nil {
		return err
	}

	// LevelDB default: запускаем compaction L0→L1 при >= 4 файлах на L0.
	const l0CompactionTrigger = 4
	if len(e.levels[0]) >= l0CompactionTrigger {
		select {
		case e.compactCh <- 0:
		default:
		}
	}

	return nil
}

// compactLevel выполняет leveled compaction: уровень N → уровень N+1.
func (e *LSMEngine) compactLevel(level int) error {
	e.mu.Lock()
	if len(e.levels) <= level || len(e.levels[level]) == 0 {
		e.mu.Unlock()
		return nil
	}
	// Round-robin: выбираем следующий файл на уровне, чтобы не compaction'ить
	// одни и те же hot keys повторно, а равномерно обрабатывать весь уровень.
	files := e.levels[level]
	idx := int(e.compactPointer[level] % uint64(len(files)))
	victim := files[idx]
	e.compactPointer[level] = uint64(idx+1) % uint64(len(files))

	// Находим overlapping файлы в level+1:
	var overlapping []*SSTReader
	if level+1 < len(e.levels) {
		for _, sst := range e.levels[level+1] {
			if bytes.Compare(sst.MaxKey(), victim.MinKey()) >= 0 && bytes.Compare(sst.MinKey(), victim.MaxKey()) <= 0 {
				overlapping = append(overlapping, sst)
			}
		}
	}
	e.mu.Unlock()

	// Merge sort:
	iters := []Iterator{victim.NewIterator()}
	for _, sst := range overlapping {
		iters = append(iters, sst.NewIterator())
	}
	merged := newMergingIterator(iters)

	// Пишем новые SST файлы в level+1:
	var newFiles []*SSTReader
	writer := e.newSSTWriter(level + 1)
	var prevKey []byte

	for merged.Next() {
		k, v := merged.Key(), merged.Value()

		// Пропускаем дубликаты (берём только первую = самую свежую версию):
		if bytes.Equal(k, prevKey) {
			continue
		}
		prevKey = append(prevKey[:0], k...)

		// Tombstone на последнем уровне — не записываем:
		if v == nil && level+1 == len(e.levels)-1 {
			continue
		}

		writer.Add(k, v)
		if writer.Size() >= 64*1024*1024 { // 64MB
			sst, err := finishAndOpen(writer)
			if err != nil {
				return err
			}
			newFiles = append(newFiles, sst)
			writer = e.newSSTWriter(level + 1)
		}
	}
	if writer.HasData() {
		sst, err := finishAndOpen(writer)
		if err != nil {
			return err
		}
		newFiles = append(newFiles, sst)
	}

	// Атомарная замена:
	e.mu.Lock()
	e.removeFromLevel(level, victim)
	e.removeFromLevel(level+1, overlapping...)
	e.addToLevel(level+1, newFiles...)
	e.mu.Unlock()

	// Сохраняем manifest и удаляем старые файлы:
	e.saveManifest()
	for _, sst := range append([]*SSTReader{victim}, overlapping...) {
		os.Remove(sst.path)
	}
	return nil
}

func valueOrNotFound(v []byte) ([]byte, error) {
	if v == nil { // tombstone
		return nil, ErrNotFound
	}
	return v, nil
}

type Manifest struct {
	Levels      [][]string `json:"levels"`        // levels[i] = список имён SST файлов
	NextFileNum uint64     `json:"next_file_num"` // следующий свободный номер файла
	// Номера WAL-файлов, чьи memtables ещё не flush'нуты на диск.
	// При recovery replay'им все эти WAL от старого к новому.
	WALFileNums []uint64 `json:"wal_file_nums"`
}

// allocFileNum возвращает следующий уникальный номер файла.
func (e *LSMEngine) allocFileNum() uint64 {
	return e.nextFileNum.Add(1)
}

// walPath формирует путь к WAL файлу по его номеру.
func walPathForNum(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.wal", num))
}

// walFileNums возвращает копию текущих номеров WAL файлов.
func (e *LSMEngine) walFileNums() []uint64 {
	nums := make([]uint64, len(e.walNums))
	copy(nums, e.walNums)
	return nums
}

func (e *LSMEngine) saveManifest() error {
	m := Manifest{
		NextFileNum: e.nextFileNum.Load(),
		WALFileNums: e.walFileNums(),
	}
	for _, level := range e.levels {
		var names []string
		for _, sst := range level {
			names = append(names, sst.path)
		}
		m.Levels = append(m.Levels, names)
	}
	data, _ := json.Marshal(m)
	// Атомарная запись: сначала во временный файл, потом rename:
	tmp := filepath.Join(e.dir, "MANIFEST.tmp")
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(e.dir, "MANIFEST"))
}

func loadManifest(dir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if os.IsNotExist(err) {
		return &Manifest{}, nil // первый запуск
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	return &m, json.Unmarshal(data, &m)
}

func (e *LSMEngine) removeFromLevel(level int, victims ...*SSTReader) {
	files := e.levels[level]
	result := make([]*SSTReader, 0, len(files))
	remove := make(map[*SSTReader]bool, len(victims))
	for _, v := range victims {
		remove[v] = true
	}
	for _, f := range files {
		if !remove[f] {
			result = append(result, f)
		}
	}
	e.levels[level] = result
}

func (e *LSMEngine) addToLevel(level int, files ...*SSTReader) {
	// Расширяем levels если нужно:
	for len(e.levels) <= level {
		e.levels = append(e.levels, nil)
	}
	e.levels[level] = append(e.levels[level], files...)

	// L1+ должен быть отсортирован по MinKey:
	if level > 0 {
		sort.Slice(e.levels[level], func(i, j int) bool {
			return bytes.Compare(
				e.levels[level][i].MinKey(),
				e.levels[level][j].MinKey(),
			) < 0
		})
	}
}

// finishAndOpen завершает запись SST и сразу открывает его для чтения.
func finishAndOpen(w *SSTWriter) (*SSTReader, error) {
	path := w.Path()
	if err := w.Finish(); err != nil {
		return nil, err
	}
	return OpenSST(path)
}

func (e *LSMEngine) newSSTWriter(level int) *SSTWriter {
	num := e.allocFileNum()
	path := filepath.Join(e.dir, fmt.Sprintf("%06d-l%d.sst", num, level))
	w, err := NewSSTWriter(path)
	if err != nil {
		panic(err) // в production — возвращать error
	}
	return w
}
