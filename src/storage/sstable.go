package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/klauspost/compress/zstd"
)

// Физический формат SST файла:
//
//   [ Data Block 0 ][ Data Block 1 ]...[ Data Block N ]
//   [ Index Block  ]
//   [ Bloom Filter ]
//   [ Footer: 24 bytes ]
//
// Footer:
//   index_offset  uint64
//   index_size    uint32
//   bloom_offset  uint64
//   bloom_size    uint32
//
// Data Block (каждый ~4KB, сжатый zstd):
//   [entry][entry]...[entry]
//
// Entry внутри Data Block:
//   shared_prefix_len  uint32  // сколько байт совпадает с предыдущим ключом
//   key_suffix_len     uint32  // длина уникальной части ключа
//   value_len          uint32
//   key_suffix         []byte
//   value              []byte
//
// Index Block:
//   Для каждого Data Block: последний ключ блока + его offset + size

const (
	blockSize  = 4 * 1024 // 4KB target block size
	footerSize = 24
)

type indexEntry struct {
	lastKey []byte
	offset  int64
	size    int32
}

// SSTWriter записывает ключи в SST файл в отсортированном порядке.
//
// Пример жизненного цикла:
//
//	w, _ := NewSSTWriter("level0/0001.sst")
//	w.Add([]byte("user:alice"), []byte("v1"))
//	w.Add([]byte("user:bob"),   []byte("v2"))
//	w.Add([]byte("user:carol"), []byte("v3"))
//	w.Finish()
//
// Add буферизует записи в block (до 4KB), затем при flushBlock
// сжимает блок zstd и записывает в файл. Для каждого сброшенного блока
// в index добавляется entry с последним ключом, offset и размером.
//
//	Add("alice") → Add("bob") → ... достигли 4KB ...
//	       │              │
//	       ▼              ▼
//	┌──────────────────────────────────────┐
//	│   block (незавершённый)              │
//	│  [entry alice][entry bob][...]       │
//	└──────────────────┬───────────────────┘
//	                   │ flushBlock
//	                   ▼
//	┌──────────────────────────────────────┐
//	│ zstd compressed → запись в файл      │
//	└──────────────────────────────────────┘
//	       +
//	index = [{lastKey: "bob", offset: 0, size: N}, ...]
type SSTWriter struct {
	file        *os.File
	buf         *bufio.Writer
	index       []indexEntry
	bloom       *BloomFilter
	block       []byte // Текущий незавершенный блок
	prevKey     []byte
	blockOffset int64
	hasFirstKey bool // Защита от пропуска порядка при первом пустом ключе
	zstd        *zstd.Encoder
	err         error // Sticky error: после первой ошибки writer перестаёт работать
}

func NewSSTWriter(path string) (*SSTWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &SSTWriter{
		file:  file,
		buf:   bufio.NewWriterSize(file, 1<<20), // 1MB
		bloom: NewBloomFilter(100_000, 0.01),
		zstd:  enc,
	}, nil
}

// Close идемпотентен и корректно отрабатывает в обоих случаях:
//   - после Finish (файл уже закрыт)
//   - вместо Finish, при ошибке по пути (defer для cleanup)
func (w *SSTWriter) Close() error {
	var firstErr error

	if w.zstd != nil {
		_ = w.zstd.Close()
		w.zstd = nil
	}

	if w.file != nil {
		// Не флашим buf — файл может быть в неконсистентном состоянии,
		// сбрасывать недописанный footer ничем не лучше обрезанного файла.
		if err := w.file.Close(); err != nil {
			firstErr = err
		}
		w.file = nil
	}

	return firstErr
}

// Size возвращает приблизительный размер записанных данных (сжатые блоки + текущий незавершённый блок).
func (w *SSTWriter) Size() int64 {
	return w.blockOffset + int64(len(w.block))
}

// HasData возвращает true если хотя бы один ключ был записан.
func (w *SSTWriter) HasData() bool {
	return w.hasFirstKey
}

// Path возвращает путь к файлу, в который пишет writer.
func (w *SSTWriter) Path() string {
	return w.file.Name()
}

// Add добавляет ключ-значение в текущий блок.
//
// Ключи должны быть строго возрастающими — это инвариант SST.
// Нарушение порядка возвращает ошибку и делает writer нерабочим (sticky error).
//
// Алгоритм:
//
//  1. Проверка порядка: key > prevKey (или первый ключ)
//  2. Добавить key в bloom filter (для быстрых negative-lookup при чтении)
//  3. Вычислить длину общего префикса с prevKey (prefix compression)
//  4. Записать entry в block: [shared(4)][suffix_len(4)][val_len(4)][suffix][value]
//  5. Если len(block) >= 4KB → flushBlock
//
// Prefix compression (шаг 3):
//
//	prevKey = "user:alice"
//	key     = "user:bob"
//	          "user:"      ← 5 байт общих (shared=5)
//
//	Сохраняем: shared=5, suffix="bob" (3 байта) вместо "user:bob" (8 байт)
//	Первый ключ блока всегда имеет shared=0 (prevKey сбрасывается в flushBlock).
func (w *SSTWriter) Add(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.hasFirstKey && bytes.Compare(w.prevKey, key) >= 0 {
		w.err = fmt.Errorf("keys must be strictly ascending and unique: prev=%q new=%q",
			w.prevKey, key)
		return w.err
	}

	w.bloom.Add(key)

	prefix := commonPrefixLen(w.prevKey, key)
	suffix := key[prefix:]

	// entry: [shared_len(4)][suffix_len(4)][val_len(4)][suffix][value]
	entry := make([]byte, 4+4+4+len(suffix)+len(value))
	binary.LittleEndian.PutUint32(entry[0:4], uint32(prefix))
	binary.LittleEndian.PutUint32(entry[4:8], uint32(len(suffix)))
	binary.LittleEndian.PutUint32(entry[8:12], uint32(len(value)))
	copy(entry[12:], suffix)
	copy(entry[12+len(suffix):], value)

	w.block = append(w.block, entry...)

	w.prevKey = append(w.prevKey[:0], key...)
	w.hasFirstKey = true

	if len(w.block) >= blockSize {
		if err := w.flushBlock(); err != nil {
			w.err = err
			return err
		}
	}

	return nil
}

// flushBlock сжимает текущий block zstd и записывает его в файл.
//
//	block (raw, ~4KB)
//	  │
//	  ▼  zstd.EncodeAll
//	compressed (~1–2KB при текстовых данных)
//	  │
//	  ▼  buf.Write
//	файл: [...][Data Block N]
//	  +
//	index = append(index, {lastKey: prevKey, offset: blockOffset, size: len(compressed)})
//
// Важно: prevKey сбрасывается в nil после flush, поэтому первый entry
// следующего блока запишет полный ключ (shared=0). Это упрощает декодирование:
// каждый блок самодостаточен и может быть декодирован независимо.
func (w *SSTWriter) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}

	compressed := w.zstd.EncodeAll(w.block, nil)
	offset := w.blockOffset

	if _, err := w.buf.Write(compressed); err != nil {
		return err
	}

	w.index = append(w.index, indexEntry{
		lastKey: append([]byte{}, w.prevKey...),
		offset:  offset,
		size:    int32(len(compressed)),
	})

	w.blockOffset += int64(len(compressed))
	w.block = w.block[:0]
	w.prevKey = w.prevKey[:0]

	return nil
}

// Finish записывает Index Block, Bloom Filter и Footer, затем закрывает файл.
//
// Финальная структура файла после Finish:
//
//	Offset 0:
//	┌──────────────────────────────────────────────┐
//	│  Data Block 0  (zstd compressed)             │
//	├──────────────────────────────────────────────┤
//	│  Data Block 1  (zstd compressed)             │
//	├──────────────────────────────────────────────┤
//	│  ...                                         │
//	├──────────────────────────────────────────────┤  ← indexOffset
//	│  Index Block                                 │
//	│  [key_len(4)][key][offset(8)][size(4)]  × N  │
//	├──────────────────────────────────────────────┤  ← bloomOffset
//	│  Bloom Filter (сериализованный)              │
//	├──────────────────────────────────────────────┤  ← size-24
//	│  Footer (24 байта)                           │
//	│  [index_offset(8)][index_size(4)]            │
//	│  [bloom_offset(8)][bloom_size(4)]            │
//	└──────────────────────────────────────────────┘
//
// Footer всегда в конце файла — это позволяет OpenSST читать его без
// знания структуры файла: достаточно ReadAt(size-24) чтобы найти всё остальное.
//
// После Finish вызывать Add нельзя. Если где-то по дороге произошла ошибка,
// writer уже в состоянии err (sticky) и Finish вернёт ту же ошибку.
func (w *SSTWriter) Finish() error {
	if w.err != nil {
		return w.err
	}

	if err := w.flushBlock(); err != nil {
		w.err = err
		return err
	}

	indexOffset := w.blockOffset

	// indexEntry → [key_len(4)][key][offset(8)][size(4)]
	// Index block: для каждого entry → [key_len(4)][key][offset(8)][size(4)].
	// Пишем напрямую в bufio через переиспользуемые буферы, без аллокаций per-entry.
	var hdr [4]byte
	var fixed [12]byte
	indexSize := int64(0)
	for _, entry := range w.index {
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(entry.lastKey)))
		if _, err := w.buf.Write(hdr[:]); err != nil {
			w.err = err
			return err
		}
		if _, err := w.buf.Write(entry.lastKey); err != nil {
			w.err = err
			return err
		}
		binary.LittleEndian.PutUint64(fixed[0:8], uint64(entry.offset))
		binary.LittleEndian.PutUint32(fixed[8:12], uint32(entry.size))
		if _, err := w.buf.Write(fixed[:]); err != nil {
			w.err = err
			return err
		}
		indexSize += int64(4 + len(entry.lastKey) + 12)
	}

	bloomOffset := indexOffset + indexSize
	bloomBytes := serializeBloom(w.bloom)
	if _, err := w.buf.Write(bloomBytes); err != nil {
		w.err = err
		return err
	}

	// index_offset, index_size, bloom_offset, bloom_size
	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(indexSize))
	binary.LittleEndian.PutUint64(footer[12:20], uint64(bloomOffset))
	binary.LittleEndian.PutUint32(footer[20:24], uint32(len(bloomBytes)))

	if _, err := w.buf.Write(footer); err != nil {
		w.err = err
		return err
	}

	if err := w.buf.Flush(); err != nil {
		w.err = err
		return err
	}

	if err := w.file.Sync(); err != nil {
		w.err = err
		return err
	}

	if err := w.file.Close(); err != nil {
		w.err = err
		return err
	}
	w.file = nil // ← пометить что файл закрыт

	if w.zstd != nil {
		_ = w.zstd.Close()
		w.zstd = nil
	}

	return nil
}

// SSTReader читает из SST файла.
//
// Index Block и Bloom Filter загружаются в память при OpenSST и остаются там
// на всё время жизни reader'а. Data Blocks читаются с диска лениво —
// только при Get или итерации.
//
//	Memory layout после OpenSST:
//
//	┌────────────────────────────────────────────────────┐
//	│ SSTReader                                          │
//	│  index: [{lastKey:"bob", off:0, sz:1024},          │
//	│          {lastKey:"zoo", off:1024, sz:980}]        │
//	│  bloom:  <BloomFilter>  (все ключи файла)          │
//	│  minKey: "alice"  (кэш первого ключа файла)        │
//	│  maxKey: "zoo"    (кэш последнего ключа файла)     │
//	│  file:   *os.File (открыт, Data Blocks на диске)   │
//	└────────────────────────────────────────────────────┘
type SSTReader struct {
	file  *os.File
	index []indexEntry
	bloom *BloomFilter
	size  int64
	zstd  *zstd.Decoder
	path  string

	minKey []byte // первый ключ файла, кэшируется при открытии
	maxKey []byte // последний ключ файла (из последнего indexEntry)
}

// OpenSST открывает SST файл и загружает в память Index Block и Bloom Filter.
//
// Data Blocks не читаются при открытии — они подгружаются лениво при Get и Seek.
//
// Алгоритм:
//
//  1. ReadAt(size-24): прочитать Footer с конца файла
//  2. По footer.indexOffset/indexSize: прочитать Index Block целиком
//  3. Распарсить index → []indexEntry{lastKey, offset, size}
//  4. По footer.bloomOffset/bloomSize: прочитать и десериализовать Bloom Filter
//  5. Кэшировать maxKey (lastKey последнего indexEntry — без disk I/O)
//     и minKey (первый ключ первого data block — требует чтения блока)
//
//	Чтение footer:
//	┌──────────────────────────────────────────────────┐
//	│ файл:  [...data...][index][bloom][footer 24B]    │
//	│                                      ▲           │
//	│              file.ReadAt(buf, size-24)           │
//	└──────────────────────────────────────────────────┘
//
// minKey кэшируется отдельно — его нет в index (там только lastKey каждого блока),
// поэтому нужно прочитать и распаковать первый data block, взяв первый entry (shared=0).
func OpenSST(path string) (reader *SSTReader, retErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = file.Close()
		}
	}()

	stats, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if stats.Size() < footerSize {
		return nil, fmt.Errorf("file too small to be SST: size=%d, footer=%d",
			stats.Size(), footerSize)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}

	// Footer:
	//   index_offset  uint64    [0:8]
	//   index_size    uint32    [8:12]
	//   bloom_offset  uint64    [12:20]
	//   bloom_size    uint32    [20:24]
	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, stats.Size()-footerSize); err != nil {
		return nil, err
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexSize := binary.LittleEndian.Uint32(footer[8:12])
	bloomOffset := binary.LittleEndian.Uint64(footer[12:20])
	bloomSize := binary.LittleEndian.Uint32(footer[20:24])

	indexBlock := make([]byte, indexSize)
	if _, err := file.ReadAt(indexBlock, int64(indexOffset)); err != nil {
		return nil, err
	}

	var indexEntries []indexEntry
	pos := 0
	// indexEntry → [key_len(4)][key][offset(8)][size(4)]
	for pos < len(indexBlock) {
		lastKeyLen := int(binary.LittleEndian.Uint32(indexBlock[pos : pos+4]))
		pos += 4
		key := append([]byte{}, indexBlock[pos:pos+lastKeyLen]...)
		pos += lastKeyLen
		offset := int64(binary.LittleEndian.Uint64(indexBlock[pos : pos+8]))
		pos += 8
		size := int32(binary.LittleEndian.Uint32(indexBlock[pos : pos+4]))
		pos += 4
		indexEntries = append(indexEntries, indexEntry{
			lastKey: key,
			offset:  offset,
			size:    size,
		})
	}

	bloomBytes := make([]byte, bloomSize)
	if _, err := file.ReadAt(bloomBytes, int64(bloomOffset)); err != nil {
		return nil, err
	}

	bloomFilter, err := deserializeBloom(bloomBytes)
	if err != nil {
		return nil, err
	}

	r := &SSTReader{
		file:  file,
		index: indexEntries,
		bloom: bloomFilter,
		size:  stats.Size(),
		zstd:  decoder, // todo не safe для concurrent DecodeAll, поэтому держим пул: каждый Get берёт свободный decoder, кладёт обратно после использования.
		path:  path,
	}

	// Кэшируем minKey и maxKey при открытии, чтобы не читать блоки при каждом обращении.
	if len(indexEntries) > 0 {
		r.maxKey = indexEntries[len(indexEntries)-1].lastKey

		// MinKey — первый entry первого блока (shared=0, suffix == полный ключ).
		entry := indexEntries[0]
		compressed := make([]byte, entry.size)
		if _, err := file.ReadAt(compressed, entry.offset); err != nil {
			return nil, fmt.Errorf("read first block for minKey: %w", err)
		}
		block, err := decoder.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("decompress first block for minKey: %w", err)
		}
		_, suffix, _, _, err := decodeEntry(block, 0)
		if err != nil {
			return nil, fmt.Errorf("decode first entry for minKey: %w", err)
		}
		r.minKey = append([]byte{}, suffix...)
	}

	return r, nil
}

func (r *SSTReader) MinKey() []byte { return r.minKey }
func (r *SSTReader) MaxKey() []byte { return r.maxKey }

func (r *SSTReader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	// Decoder'ы из пула просто отпускаем под GC — sync.Pool не даёт корректно
	// итерировать содержимое, а закрывать вручную не имеет смысла, пока процесс жив.
	return err
}

// Get ищет ключ в SST файле за три фазы:
//
//  1. Bloom filter: может ли ключ вообще быть в файле?
//     Если нет — немедленно ErrNotFound без обращения к диску.
//
//  2. Binary search по index: первый блок с lastKey >= key.
//     Index целиком в памяти → O(log N) без disk I/O.
//
//  3. Загрузить Data Block с диска, распаковать zstd,
//     линейно пройти по prefix-compressed записям до совпадения.
//
//	Пример: Get("carol")
//
//	index:  [{lastKey:"bob"}, {lastKey:"zoo"}, ...]
//	           < "carol"         >= "carol"
//	          left++    →  blockIdx=1
//
//	Блок 1, линейный поиск с начала:
//	  shared=0, suffix="alice" → prevKey="alice"  < "carol", continue
//	  shared=5, suffix="ob"   → prevKey="user:ob"   ...
//	  shared=0, suffix="carol" → prevKey="carol"  == "carol", return val ✓
//
// Tombstone (val == tombstoneVal) трактуется как ErrNotFound —
// ключ был удалён, но запись физически ещё присутствует в файле
// до следующего compaction.
func (r *SSTReader) Get(key []byte) ([]byte, error) {
	if !r.bloom.MayContain(key) {
		return nil, ErrNotFound
	}

	left, right := 0, len(r.index)-1
	blockIdx := -1
	for left <= right {
		middle := (left + right) / 2
		if bytes.Compare(r.index[middle].lastKey, key) < 0 {
			left = middle + 1
		} else {
			blockIdx = middle
			right = middle - 1
		}
	}

	if blockIdx == -1 {
		return nil, ErrNotFound
	}

	entry := r.index[blockIdx]
	compressed := make([]byte, entry.size)
	if _, err := r.file.ReadAt(compressed, entry.offset); err != nil {
		return nil, err
	}

	block, err := r.zstd.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}

	pos := 0
	var prevKey []byte
	for pos < len(block) {
		shared, suffix, val, newPos, err := decodeEntry(block, pos)
		if err != nil {
			return nil, err
		}
		pos = newPos

		prevKey = append(prevKey[:shared], suffix...)

		if bytes.Equal(prevKey, key) {
			if string(val) == tombstoneVal {
				return nil, ErrNotFound
			}
			return append([]byte{}, val...), nil
		}
		// Оптимизация: если текущий ключ уже больше искомого — стоп
		if bytes.Compare(prevKey, key) > 0 {
			break
		}
	}

	return nil, ErrNotFound
}

func (r *SSTReader) NewIterator() *SSTIterator {
	it := &SSTIterator{
		reader: r,
	}
	if len(r.index) == 0 {
		return it
	}
	if err := it.loadBlock(0); err != nil {
		it.err = err
		return it
	}
	it.advanceInBlock()
	return it
}

// decodeEntry декодирует одну запись из блока начиная с позиции pos.
// Возвращает shared prefix length, suffix, value и новую позицию.
func decodeEntry(block []byte, pos int) (shared int, suffix, value []byte, newPos int, err error) {
	if pos+12 > len(block) {
		return 0, nil, nil, pos, fmt.Errorf("corrupted block: truncated entry header at offset %d", pos)
	}
	shared = int(binary.LittleEndian.Uint32(block[pos : pos+4]))
	pos += 4
	suffixLen := int(binary.LittleEndian.Uint32(block[pos : pos+4]))
	pos += 4
	valLen := int(binary.LittleEndian.Uint32(block[pos : pos+4]))
	pos += 4
	if pos+suffixLen+valLen > len(block) {
		return 0, nil, nil, pos, fmt.Errorf("corrupted block: entry payload exceeds block bounds at offset %d", pos)
	}
	suffix = block[pos : pos+suffixLen]
	pos += suffixLen
	value = block[pos : pos+valLen]
	pos += valLen
	return shared, suffix, value, pos, nil
}

// commonPrefixLen возвращает длину общего префикса двух байтовых слайсов.
func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

type Iterator interface {
	// Позиционирование
	SeekToFirst()       // установиться на первый ключ
	Seek(target []byte) // первый ключ >= target
	Next()              // следующий ключ

	// Доступ к текущей позиции
	Valid() bool   // есть ли валидный элемент на текущей позиции
	Key() []byte   // ключ текущего элемента
	Value() []byte // значение текущего элемента
	Err() error    // накопленная ошибка, если была

	Close() error
}

// SSTIterator обходит все записи SST файла в отсортированном порядке.
//
// Итератор позиционируется на блок (blockIdx) и внутри него — на байтовую
// позицию (pos). advanceInBlock читает следующий prefix-compressed entry
// и восстанавливает полный ключ: curKey = curKey[:shared] + suffix.
//
//	Состояние итератора (пример: стоит на "carol" в блоке 1):
//
//	┌────────────────────────────────────────────────────────┐
//	│ SSTIterator                                            │
//	│  blockIdx: 1          ← индекс текущего блока в index  │
//	│  block:    [...]      ← распакованный Data Block 1     │
//	│  pos:      87         ← следующий entry начинается тут │
//	│  curKey:   "carol"    ← текущий ключ                   │
//	│  curValue: "v3"       ← текущее значение               │
//	│  valid:    true                                        │
//	└────────────────────────────────────────────────────────┘
type SSTIterator struct {
	reader *SSTReader

	// Текущий блок: индекс и распакованные данные
	blockIdx int    // Индекс в reader.index. -1, если итератор невалиден
	block    []byte // Распакованные данные текущего блока
	pos      int    // Позиция в block, откуда читать следующий entry

	// Текущая позиция
	curKey   []byte
	curValue []byte

	// Состояние
	err   error
	valid bool
}

func (it *SSTIterator) Valid() bool {
	return it.err == nil && it.valid
}

func (it *SSTIterator) Key() []byte {
	return it.curKey
}

func (it *SSTIterator) Value() []byte {
	return it.curValue
}

func (it *SSTIterator) Err() error {
	return it.err
}

func (it *SSTIterator) Close() error {
	it.block = nil
	it.curKey = nil
	it.curValue = nil
	it.valid = false
	return nil
}

// Next перемещает итератор к следующей записи.
//
// Два случая:
//
//  1. Fast path: есть ещё entry в текущем блоке → advanceInBlock (только RAM).
//  2. Slow path: блок кончился → loadBlock(blockIdx+1) + advanceInBlock (disk I/O).
//
//	Блок 0:  [alice][bob][carol]   ← pos достиг конца блока
//	                                      │
//	                               loadBlock(1)  ← читаем с диска
//	                                      │
//	Блок 1:  [dave][eve][frank]    ← pos=0, curKey сбрасывается,
//	                                  advanceInBlock → curKey="dave"
func (it *SSTIterator) Next() {
	if !it.valid {
		return
	}

	if !it.advanceInBlock() {
		if it.blockIdx+1 >= len(it.reader.index) {
			it.valid = false
			return
		}

		if err := it.loadBlock(it.blockIdx + 1); err != nil {
			it.err = err
			it.valid = false
			return
		}

		it.curKey = it.curKey[:0]
		it.advanceInBlock()
	}
}

func (it *SSTIterator) SeekToFirst() {
	it.curKey = it.curKey[:0]
	it.err = nil
	it.valid = false
	if len(it.reader.index) == 0 {
		return
	}
	if err := it.loadBlock(0); err != nil {
		it.err = err
		return
	}
	it.advanceInBlock()
}

// Seek позиционирует итератор на первый ключ >= target.
//
// Алгоритм:
//
//  1. Binary search по index: первый блок с lastKey >= target. O(log N).
//  2. loadBlock: загрузить и распаковать блок с диска.
//  3. Линейный поиск с начала блока: идём entry за entry, пока curKey < target.
//
//	Пример: Seek("carol")
//
//	index:  [{lastKey:"bob"}, {lastKey:"zoo"}, ...]
//	                               ▲ blockIdx=1
//
//	Линейный поиск в блоке 1:
//	  curKey="" → advanceInBlock → "alice"  < "carol" → continue
//	             → advanceInBlock → "bob"    < "carol" → continue
//	             → advanceInBlock → "carol"  >= "carol" → stop ✓
//
// Если все блоки имеют lastKey < target — blockIdx=-1, valid=false.
func (it *SSTIterator) Seek(target []byte) {
	left, right := 0, len(it.reader.index)-1
	blockIdx := -1

	for left <= right {
		middle := (left + right) / 2
		if bytes.Compare(it.reader.index[middle].lastKey, target) < 0 {
			left = middle + 1
		} else {
			blockIdx = middle
			right = middle - 1
		}
	}

	if blockIdx == -1 {
		it.valid = false
		return
	}

	if err := it.loadBlock(blockIdx); err != nil {
		it.err = err
		it.valid = false
		return
	}

	// 3. Линейный поиск с начала блока до ключа >= target
	it.curKey = it.curKey[:0]
	for {
		if !it.advanceInBlock() {
			// Блок кончился — теоретически не должно произойти (индекс сказал,
			// что lastKey блока >= target), но на всякий случай.
			return
		}
		if bytes.Compare(it.curKey, target) >= 0 {
			return
		}
	}
}

// loadBlock загружает и распаковывает Data Block с индексом idx.
//
//  1. file.ReadAt: читаем entry.size сжатых байт с offset entry.offset
//  2. zstd.DecodeAll: распаковываем в it.block
//  3. Сбрасываем pos=0, blockIdx=idx
//
// После loadBlock позиция внутри блока стоит на начале (pos=0).
// curKey не изменяется — caller сам решает, когда его сбрасывать:
//   - Next и SeekToFirst сбрасывают curKey в nil (новый блок, новый контекст)
//   - Seek не сбрасывает: продолжает линейный поиск с пустым curKey явно
func (it *SSTIterator) loadBlock(idx int) error {
	entry := it.reader.index[idx]
	compressed := make([]byte, entry.size)
	if _, err := it.reader.file.ReadAt(compressed, entry.offset); err != nil {
		return err
	}

	block, err := it.reader.zstd.DecodeAll(compressed, nil)
	if err != nil {
		return err
	}

	it.block = block
	it.pos = 0
	it.blockIdx = idx

	return nil
}

// advanceInBlock читает следующий entry из текущего блока
// и применяет prefix decompression к curKey.
//
// Prefix decompression:
//
//	entry: shared=5, suffix="bob"
//	curKey до:    "user:alice"
//	curKey после: curKey[:5] + "bob" = "user:bob"
//
//	entry: shared=0, suffix="zoo"
//	curKey до:    "user:bob"
//	curKey после: curKey[:0] + "zoo" = "zoo"
//
// curKey переиспользуется через append(curKey[:shared], suffix...),
// что позволяет избежать аллокации на каждом шаге.
//
// Возвращает false, если блок кончился (pos >= len(block)) или entry битый.
func (it *SSTIterator) advanceInBlock() bool {
	if it.pos >= len(it.block) {
		it.valid = false
		return false
	}

	shared, suffix, value, newPos, err := decodeEntry(it.block, it.pos)
	if err != nil {
		it.err = err
		it.valid = false
		return false
	}
	it.pos = newPos

	it.curKey = append(it.curKey[:shared], suffix...)
	it.curValue = append(it.curValue[:0], value...)

	it.valid = true
	return true
}
