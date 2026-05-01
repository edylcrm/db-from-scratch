package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Формат одной записи на диске:
//
//  ┌──────────┬─────────┬─────────┬──────────┬────────────┐
//  │ CRC32    │ key_len │ val_len │ key      │ value      │
//  │ 4 bytes  │ 4 bytes │ 4 bytes │ N bytes  │ M bytes    │
//  └──────────┴─────────┴─────────┴──────────┴────────────┘
//
//  CRC32 считается от [key_len || val_len || key || value].
//  Если при чтении CRC не совпадает — запись битая (краш в середине write).
//
//  Пример записи Set("name", "alice"):
//
//  CRC32          key_len=4     val_len=5     key           value
//  [XX XX XX XX] [04 00 00 00] [05 00 00 00] [6E 61 6D 65] [61 6C 69 63 65]
//                                             n  a  m  e    a  l  i  c  e
//
//  Все числовые поля записаны в Little Endian (младший байт первым),
//  поэтому key_len=4 выглядит как [04 00 00 00], а не [00 00 00 04].

const (
	// recordHeaderSize — суммарный размер фиксированной части записи:
	// CRC32 (4 байта) + key_len (4 байта) + val_len (4 байта) = 12 байт.
	recordHeaderSize = 12

	// tombstoneVal — специальное значение, которое записывается вместо value
	// при удалении ключа. Append-only файл не позволяет удалять данные
	// из середины, поэтому Delete — это обычный Set с маркером-tombstone.
	// При чтении, если value == tombstoneVal, ключ считается удалённым.
	tombstoneVal = "\x00TOMBSTONE\x00"
)

// ErrNotFound возвращается из Get, если ключ не найден
// или был удалён (последняя запись — tombstone).
var ErrNotFound = errors.New("key not found")

// Log — append-only хранилище ключ-значение на основе одного файла.
//
// Все записи дописываются в конец файла (O_APPEND).
// Чтение — полный скан файла от начала до конца (O(n)).
//
// Потокобезопасен: запись защищена exclusive lock (mu.Lock),
// чтение — shared lock (mu.RLock). Несколько горутин могут
// читать одновременно, но запись блокирует и чтение, и другие записи.
type Log struct {
	mu   sync.RWMutex
	file *os.File
}

// Open открывает (или создаёт) файл для append-only лога.
//
// Флаги:
//   - O_RDWR   — чтение и запись (нужно и для Get, и для Set)
//   - O_CREATE — создать файл, если не существует
//   - O_APPEND — все Write будут дописывать в конец файла;
//     ОС гарантирует это даже если Seek сдвинул позицию чтения
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Log{file: f}, nil
}

// Close закрывает файл. После вызова Log использовать нельзя.
func (l *Log) Close() error {
	return l.file.Close()
}

// Set записывает пару ключ-значение в конец файла.
// Если ключ уже существует — новая запись «перекроет» старую:
// Get всегда возвращает значение из последней записи.
func (l *Log) Set(key, value []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writeRecord(key, value)
}

// Delete помечает ключ как удалённый, записывая tombstone.
// Физически данные остаются в файле — это особенность append-only подхода.
func (l *Log) Delete(key []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writeRecord(key, []byte(tombstoneVal))
}

// writeRecord формирует бинарную запись и дописывает её в файл.
//
// Шаги:
//  1. Собрать payload: [key_len | val_len | key | value]
//  2. Вычислить CRC32 от payload
//  3. Собрать полную запись: [CRC32 | payload]
//  4. Записать в файл одним вызовом Write
func (l *Log) writeRecord(key, value []byte) error {
	// payload — всё, что покрывается контрольной суммой:
	//
	//  ┌─────────┬─────────┬──────────┬────────────┐
	//  │ key_len │ val_len │ key      │ value      │
	//  │ 4 bytes │ 4 bytes │ N bytes  │ M bytes    │
	//  └─────────┴─────────┴──────────┴────────────┘
	//  |<--------------- payload ----------------->|
	payload := make([]byte, 8+len(key)+len(value))

	// Записываем длину в первые 8 байт payload (Little Endian).
	// binary.LittleEndian.PutUint32 кладёт uint32 в слайс,
	// располагая младший байт по младшему адресу.
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(value)))

	// Копируем ключ и значение после заголовка.
	copy(payload[8:], key)
	copy(payload[8+len(key):], value)

	// CRC32 вычисляется от всего payload целиком (key_len + val_len + key + value).
	checksum := crc32.ChecksumIEEE(payload)

	// Собираем полную запись: CRC32 (4 байта) + payload.
	//
	//  ┌──────────┬────────────────────────────────┐
	//  │ CRC32    │           payload              │
	//  │ 4 bytes  │  8 + len(key) + len(value)     │
	//  └──────────┴────────────────────────────────┘
	//  |<---------------- record ----------------->|
	record := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(record[0:4], checksum)
	copy(record[4:], payload)

	// Один вызов Write = одна системная операция записи.
	// O_APPEND гарантирует, что данные попадут в конец файла,
	// даже если позиция чтения была сдвинута через Seek.
	if _, err := l.file.Write(record); err != nil {
		return err
	}

	return nil
}

// Get читает весь файл и возвращает последнее значение для ключа.
//
// Сложность O(n) — нужно просканировать все записи, потому что
// у нас нет индекса. Последняя запись для ключа — актуальная,
// все предыдущие — устаревшие версии.
//
// Если последняя запись для ключа — tombstone, возвращаем ErrNotFound.
func (l *Log) Get(key []byte) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []byte
	found := false

	// Сканируем все записи от начала файла.
	// Для каждой записи с совпадающим ключом обновляем result.
	// В итоге result будет содержать значение из последней записи.
	err := l.scan(func(k, v []byte, _ int64) error {
		if string(k) == string(key) {
			if string(v) == tombstoneVal {
				// Tombstone — ключ удалён. Сбрасываем результат,
				// но продолжаем сканировать: после Delete мог быть
				// новый Set с тем же ключом.
				result = nil
				found = false
			} else {
				result = v
				found = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound
	}
	return result, nil
}

// scan итерирует все записи файла от начала до конца.
// Для каждой валидной записи вызывает fn(key, value, offset).
//
// offset — смещение записи в байтах от начала файла.
// Это позволяет вызывающему коду запомнить, где именно лежит запись,
// чтобы потом прочитать её напрямую через Seek, минуя полный скан.
// Например, HashDB строит in-memory индекс {key → offset} при старте.
//
// Оборванная запись в конце файла (результат краша) — молча игнорируется.
// Это корректно: если процесс упал на полузаписи, последняя запись
// будет неполной, и мы её просто пропускаем.
//
// Как работает чтение одной записи:
//
//  Файл: [CRC32 | key_len | val_len | key | value] [CRC32 | key_len | ...]
//         ^                                          ^
//         offset=0                                   offset=12+N+M
func (l *Log) scan(fn func(key, value []byte, offset int64) error) error {
	// Перематываем файл в начало перед чтением.
	// Write всегда идёт в конец (O_APPEND), а Seek влияет только
	// на позицию чтения — они не конфликтуют.
	// Seek возвращает итоговую позицию — используем её как начальный offset (0).
	offset, err := l.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// 4 (crc) + 4 (key_len) + 4 (val_len)
	header := make([]byte, recordHeaderSize)

	for {
		// --- Шаг 1: читаем заголовок (12 байт) ---
		//
		//  header[0:4]  → CRC32
		//  header[4:8]  → key_len
		//  header[8:12] → val_len
		_, err := io.ReadFull(l.file, header)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// EOF — файл закончился ровно на границе записи (нормально).
				// ErrUnexpectedEOF — файл закончился посреди заголовка
				// (оборванная запись после краша). В обоих случаях —
				// прекращаем чтение без ошибки.
				return nil
			}

			return err
		}

		checksum := binary.LittleEndian.Uint32(header[0:4])
		keyLen := binary.LittleEndian.Uint32(header[4:8])
		valLen := binary.LittleEndian.Uint32(header[8:12])

		// --- Шаг 2: собираем payload для проверки CRC ---
		//
		// payload должен быть идентичен тому, что записывался в writeRecord:
		//
		//  ┌─────────┬─────────┬──────────┬────────────┐
		//  │ key_len │ val_len │ key      │ value      │
		//  └─────────┴─────────┴──────────┴────────────┘
		//
		// Первые 8 байт (key_len, val_len) мы уже знаем из заголовка —
		// записываем их в payload вручную. Оставшиеся байты (key + value)
		// читаем из файла.
		payload := make([]byte, 8+keyLen+valLen)
		binary.LittleEndian.PutUint32(payload[0:4], keyLen)
		binary.LittleEndian.PutUint32(payload[4:8], valLen)

		// --- Шаг 3: читаем key + value из файла в payload[8:] ---
		if _, err := io.ReadFull(l.file, payload[8:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}

			return err
		}

		// --- Шаг 4: проверяем контрольную сумму ---
		//
		// Вычисляем CRC32 от payload (key_len + val_len + key + value)
		// и сравниваем с CRC32, прочитанным из заголовка.
		// Если не совпадает — запись повреждена (битый диск, неполная запись).
		// Прекращаем чтение: всё после битой записи считается ненадёжным.
		if crc32.ChecksumIEEE(payload) != checksum {
			return nil
		}

		// --- Шаг 5: вызываем callback с ключом и значением ---
		//
		//  payload: [key_len | val_len | key      | value     ]
		//            0        4         8          8+keyLen
		//                               ^          ^
		//                               |── key ──||── value ──|
		if err := fn(payload[8:8+keyLen], payload[8+keyLen:], offset); err != nil {
			return err
		}

		// Сдвигаем offset на размер только что прочитанной записи:
		// recordHeaderSize (12 байт) + длина ключа + длина значения.
		// После этого offset указывает на начало следующей записи.
		offset += int64(recordHeaderSize + keyLen + valLen)
	}
}
