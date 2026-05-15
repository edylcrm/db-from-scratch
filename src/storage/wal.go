package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// Формат одной записи в WAL:
//
//   ┌──────┬───────┬──────┬──────────────────────────────────┐
//   │ CRC  │ Len   │ Type │ Payload                          │
//   │ 4B   │ 4B    │ 1B   │ N bytes                          │
//   └──────┴───────┴──────┴──────────────────────────────────┘
//
// CRC считается от [type || payload], то есть изменение типа тоже ломает CRC.
// Len — размер только payload (без type), нужен чтобы знать сколько байт читать.
//
// Три типа записей:
//
//   Type = 0x01 (Put):
//     Payload: [key_len(4)] [val_len(4)] [key] [value]
//
//   Type = 0x02 (Delete):
//     Payload: [key_len(4)] [key]
//
//   Type = 0x03 (BatchEnd):
//     Payload: пусто (маркер конца batch-транзакции)
//
// Пример записи Put("name", "alice"):
//
//   CRC         Len=13      Type=0x01  key_len=4   val_len=5   key        value
//   [XX XX XX XX] [0D 00 00 00] [01]     [04 00 00 00] [05 00 00 00] [6E 61 6D 65] [61 6C 69 63 65]
//                                                                     n  a  m  e    a  l  i  c  e

type RecordType byte

const (
	RecordTypePut      RecordType = 0x01
	RecordTypeDelete   RecordType = 0x02
	RecordTypeBatchEnd RecordType = 0x03
)

// LSN (Log Sequence Number) — уникальный монотонно возрастающий номер записи.
//
// Используется для одной задачи: определить, какую часть WAL можно удалить.
// После flush memtable → SSTable, все записи с LSN <= flush_lsn
// уже на диске в SSTable, и WAL до этого момента можно удалить.
//
//   WAL: [LSN=1] [LSN=2] [LSN=3] [LSN=4] [LSN=5]
//                          ▲
//                    flush_lsn = 3
//                    LSN 1..3 → в SSTable, можно удалить
//                    LSN 4..5 → только в memtable, нужны в WAL
type LSN uint64

// WAL гарантирует durability: каждая запись сначала попадает на диск,
// и только потом — в memtable (память). Если процесс упал, при старте
// мы проигрываем WAL и восстанавливаем memtable.
//
// Путь записи:
//
//   Put("key", "val")
//     │
//     ├─ 1. WAL.WritePut(key, val)
//     │      │
//     │      ├─ write(record) → bufio → OS page cache
//     │      ├─ flush()       → сбросить bufio → OS page cache
//     │      └─ fsync()       → OS page cache → физический диск
//     │
//     └─ 2. memtable.Set(key, val)   ← только после успешного fsync
//
// fsync — самая дорогая операция: ~0.1ms на NVMe, ~1-10ms на HDD.
// GroupCommitWAL ниже амортизирует эту стоимость через батчинг.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer // буфер 64 КБ, уменьшает кол-во системных вызовов write()
	lsn    LSN
}

// OpenWAL открывает (или создаёт) WAL-файл.
//
// O_APPEND гарантирует, что write() всегда дописывает в конец,
// даже если Seek сдвинул позицию чтения (нужно для Recover).
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:   f,
		writer: bufio.NewWriterSize(f, 64*1024),
	}, nil
}

// Close закрывает WAL-файл.
//
// Важно: мы НЕ делаем Flush() перед закрытием. Если в bufio-буфере
// остались данные, которые не были зафиксированы через Sync() —
// они должны быть потеряны. Записывать их без fsync было бы хуже:
// клиент не получил подтверждения, но данные попали в файл, создавая
// иллюзию durability.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// WritePut записывает операцию Put в WAL с fsync.
// Возвращает LSN этой записи.
func (w *WAL) WritePut(key, value []byte) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeRecord(RecordTypePut, buildPutPayload(key, value))
}

// WriteDelete записывает операцию Delete в WAL с fsync.
func (w *WAL) WriteDelete(key []byte) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeRecord(RecordTypeDelete, buildDeletePayload(key))
}

// writeRecord — внутренний метод, вызывается под локом.
//
// Три этапа записи:
//
//   ┌──────────────────────────────────────────────────────────┐
//   │  Go process                                              │
//   │                                                          │
//   │  [record bytes]                                          │
//   │       │                                                  │
//   │       ▼  Write()                                         │
//   │  ┌──────────────┐                                        │
//   │  │ bufio.Writer │  ← буфер 64 КБ в памяти процесса      │
//   │  └──────┬───────┘                                        │
//   │         │  Flush()                                       │
//   │         ▼                                                │
//   │  ┌──────────────┐                                        │
//   │  │  OS page     │  ← RAM ядра ОС, не на диске            │
//   │  │  cache       │                                        │
//   │  └──────┬───────┘                                        │
//   │         │  Sync() (fsync)                                │
//   │         ▼                                                │
//   │  ┌──────────────┐                                        │
//   │  │  Физический  │  ← гарантированно на диске              │
//   │  │  диск        │                                        │
//   │  └──────────────┘                                        │
//   └──────────────────────────────────────────────────────────┘
func (w *WAL) writeRecord(recType RecordType, payload []byte) (LSN, error) {
	record := buildRecord(recType, payload)

	// Этап 1: запись в bufio-буфер (в памяти процесса).
	if _, err := w.writer.Write(record); err != nil {
		return 0, err
	}

	// Этап 2: сброс bufio-буфера в page cache ОС.
	// После Flush() данные в ядре ОС, но ещё не на диске.
	if err := w.writer.Flush(); err != nil {
		return 0, err
	}

	// Этап 3: fsync — сброс page cache на физический диск.
	// Только после этого данные гарантированно переживут потерю питания.
	// Это самая медленная операция: ~0.1ms на NVMe, ~1-10ms на HDD.
	if err := w.file.Sync(); err != nil {
		return 0, err
	}

	w.lsn++
	return w.lsn, nil
}

// Recover восстанавливает memtable из WAL-файла при старте процесса.
//
// Алгоритм: читаем записи от начала до конца, применяя каждую к memtable.
//
//   Старт → memtable пустая
//
//   WAL: [PUT a→1] [PUT b→2] [DEL a] [PUT c→3] [оборванная]
//          │          │         │        │          │
//          ▼          ▼         ▼        ▼          └─ CRC не сойдётся → стоп
//      Set(a,1)   Set(b,2)  Del(a)  Set(c,3)
//
//   Итог: memtable = { b→2, c→3 }
//
// Оборванная запись в конце (краш на полузаписи) игнорируется:
// CRC не сойдётся и мы прекращаем чтение.
func (w *WAL) Recover(memTable *SkipList) error {
	// Перематываем файл в начало. O_APPEND не мешает Seek для чтения —
	// O_APPEND влияет только на write().
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	for {
		// --- Читаем заголовок: [CRC(4)] [Len(4)] [Type(1)] = 9 байт ---
		header := make([]byte, 9)
		if _, err := io.ReadFull(reader, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // нормальный конец файла или оборванный заголовок
			}
			return err
		}

		crc := binary.LittleEndian.Uint32(header[0:4])
		payloadLen := binary.LittleEndian.Uint32(header[4:8])
		recType := RecordType(header[8])

		// --- Читаем payload ---
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // оборванный payload — краш в середине записи
			}
			return err
		}

		// --- Проверяем CRC ---
		// CRC покрывает [type || payload]: изменение типа тоже ломает CRC.
		// Это важно: без type в CRC можно было бы подменить тип записи
		// (например, Put → Delete), не сломав контрольную сумму.
		h := crc32.NewIEEE()
		h.Write([]byte{byte(recType)})
		h.Write(payload)
		if h.Sum32() != crc {
			return nil // битая запись — конец валидных данных
		}

		// --- Применяем запись к memtable ---
		switch recType {
		case RecordTypePut:
			key, value, err := parsePutPayload(payload)
			if err != nil {
				return nil // логически битая запись — завершаем recover
			}
			memTable.Set(key, value)

		case RecordTypeDelete:
			key, err := parseDeletePayload(payload)
			if err != nil {
				return nil
			}
			memTable.Delete(key)
		}

		w.lsn++
	}
}

// TruncateBefore удаляет WAL-записи до указанного LSN.
//
// Вызывается после flush memtable → SSTable: записи до flush_lsn
// теперь на диске в SSTable и не нужны в WAL для восстановления.
//
// Алгоритм — атомарная замена через временный файл:
//
//   1. Создаём tmp-файл
//   2. Копируем в tmp только записи с LSN >= lsn
//   3. fsync tmp
//   4. os.Rename(tmp, original) — атомарная замена на уровне ФС
//   5. Переоткрываем файл
//
//   До:
//   wal.log: [LSN=1] [LSN=2] [LSN=3] [LSN=4] [LSN=5]
//                                      ▲
//                                   lsn = 4
//
//   После:
//   wal.log: [LSN=4] [LSN=5]
//
// Почему не truncate на месте? Потому что нельзя удалить начало файла
// без переписывания. А запись в середине Rename гарантирует, что
// при краше мы либо видим старый файл, либо новый — не частичный.
func (w *WAL) TruncateBefore(lsn LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	walPath := w.file.Name()

	// Перематываем для чтения. O_APPEND не мешает.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	tmpPath := walPath + ".tmp"
	tmp, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	// commit-or-rollback: при ошибке удаляем tmp и сохраняем исходный файл.
	// Если всё прошло успешно — success=true и defer ничего не делает.
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	tmpWriter := bufio.NewWriterSize(tmp, 64*1024)
	reader := bufio.NewReader(w.file)
	currentLSN := LSN(0)

	// --- Читаем все записи и копируем только нужные ---
	for {
		header := make([]byte, 9)
		if _, err := io.ReadFull(reader, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		payloadLen := int(binary.LittleEndian.Uint32(header[4:8]))
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		// Проверяем CRC — битая запись означает конец валидных данных.
		recType := RecordType(header[8])
		storedCRC := binary.LittleEndian.Uint32(header[0:4])
		h := crc32.NewIEEE()
		h.Write([]byte{byte(recType)})
		h.Write(payload)
		if h.Sum32() != storedCRC {
			break
		}

		currentLSN++

		// Копируем только записи с LSN >= lsn (остальные уже в SSTable).
		if currentLSN >= lsn {
			if _, err := tmpWriter.Write(header); err != nil {
				return err
			}
			if _, err := tmpWriter.Write(payload); err != nil {
				return err
			}
		}
	}

	// --- Финализация: flush → fsync → close → rename → reopen ---

	if err := tmpWriter.Flush(); err != nil {
		return err
	}
	// fsync tmp-файл: данные должны быть на диске до rename,
	// иначе при краше после rename файл может быть пустым.
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	// Атомарная замена на уровне файловой системы.
	// После rename старый файл исчезает, новый занимает его место.
	// Если rename упал — исходный файл уже закрыт, но tmp ещё на диске.
	if err := os.Rename(tmpPath, walPath); err != nil {
		return err
	}

	// Переоткрываем файл: после Close + Rename старый file descriptor
	// невалиден. Нужен новый fd для последующих WritePut.
	newFile, err := os.OpenFile(walPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = newFile
	w.writer = bufio.NewWriterSize(newFile, 64*1024)

	success = true
	return nil
}

// Вместо fsync на каждую запись GroupCommitWAL собирает пачку записей
// за ~2 мс и делает один fsync на всю пачку.
//
// Зачем: fsync — узкое место. На NVMe ~0.1 мс на один fsync = максимум
// ~10 000 записей/сек при fsync на каждую. С group commit за 2 мс
// можно собрать сотни записей и сделать один fsync — пропускная
// способность вырастает на порядок.
//
// Компромисс: латентность отдельной записи увеличивается (до +2 мс
// ожидания), но пропускная способность — в десятки раз выше.
//
// Архитектура:
//
//   Горутины-писатели                  commitLoop (отдельная горутина)
//   ┌──────────┐                       ┌─────────────────────────────┐
//   │WritePut  │──┐                    │                             │
//   └──────────┘  │                    │  1. Жду записи из pending   │
//   ┌──────────┐  │  pending channel   │  2. Собираю пачку за ~2ms   │
//   │WritePut  │──┼───────────────────►│  3. Пишу всё в буфер        │
//   └──────────┘  │                    │  4. Один Flush + Sync       │
//   ┌──────────┐  │                    │  5. Уведомляю всех через    │
//   │WriteDel  │──┘                    │     done channel            │
//   └──────────┘                       └─────────────────────────────┘
//        ▲                                         │
//        │             done channel                │
//        └─────────────────────────────────────────┘
//
// GroupCommitWAL реализует тот же интерфейс, что и WAL (WritePut,
// WriteDelete, Recover, Close), поэтому LSM engine может использовать
// любую реализацию без изменения кода.

// maxBatchSize ограничивает количество записей в одном батче.
// Без лимита, при непрерывном потоке писателей, drain-цикл мог бы
// бесконечно читать из канала и никогда не дойти до fsync —
// все писатели зависли бы на своих done-каналах.

// writeResult — результат записи, который commitLoop отправляет
// обратно писателю через done channel.
type writeResult struct {
	lsn LSN
	err error
}

// pendingWrite — одна запись, ожидающая fsync в commitLoop.
//
//   record — уже сформированная бинарная запись [CRC|Len|Type|Payload],
//            готовая к записи в файл.
//   done   — канал, через который commitLoop уведомит писателя
//            о результате (LSN или ошибка). Буфер 1, чтобы
//            commitLoop не блокировался на отправке.
type pendingWrite struct {
	record []byte
	done   chan writeResult
}

type GroupCommitWAL struct {
	*WAL                       // встраиваем WAL: Recover и TruncateBefore наследуются
	pending  chan pendingWrite // канал от писателей к commitLoop
	stop     chan struct{}     // сигнал завершения для commitLoop
	loopDone chan struct{}     // закрывается когда commitLoop завершился
}

// NewGroupCommitWAL создаёт WAL с group commit.
//
// Запускает фоновую горутину commitLoop, которая живёт до вызова Close().
func NewGroupCommitWAL(path string) (*GroupCommitWAL, error) {
	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}

	gc := &GroupCommitWAL{
		WAL:      wal,
		pending:  make(chan pendingWrite, maxBatchSize),
		stop:     make(chan struct{}),
		loopDone: make(chan struct{}),
	}

	go gc.commitLoop()
	return gc, nil
}

// Close завершает commitLoop и закрывает файл.
//
// Порядок:
//   1. Сигнализируем commitLoop завершиться (close(stop))
//   2. Ждём пока commitLoop допишет оставшийся батч (<-loopDone)
//   3. Закрываем файл через WAL.Close()
//
// После Close все последующие WritePut вернут errWALClosed.
func (gc *GroupCommitWAL) Close() error {
	close(gc.stop)
	<-gc.loopDone
	return gc.WAL.Close()
}

// WritePut перехватывает вызов у встроенного *WAL.
//
// Вместо прямого write+flush+sync формирует бинарную запись
// и отправляет её в commitLoop через pending channel.
// Блокируется до тех пор, пока commitLoop не сделает fsync
// и не отправит результат в done channel.
func (gc *GroupCommitWAL) WritePut(key, value []byte) (LSN, error) {
	record := buildRecord(RecordTypePut, buildPutPayload(key, value))
	return gc.sendToLoop(record)
}

// WriteDelete — аналогично WritePut, но для операции Delete.
func (gc *GroupCommitWAL) WriteDelete(key []byte) (LSN, error) {
	record := buildRecord(RecordTypeDelete, buildDeletePayload(key))
	return gc.sendToLoop(record)
}

var errWALClosed = errors.New("wal: closed")

// sendToLoop отправляет запись в commitLoop и ждёт результат.
//
//   Писатель:
//     1. Создаёт done channel (буфер 1)
//     2. Отправляет {record, done} в pending
//     3. Блокируется на <-done
//     4. Получает {lsn, err} от commitLoop
//
// Если commitLoop уже завершился (loopDone закрыт) — возвращаем ошибку.
func (gc *GroupCommitWAL) sendToLoop(record []byte) (LSN, error) {
	done := make(chan writeResult, 1)
	select {
	case gc.pending <- pendingWrite{record: record, done: done}:
	case <-gc.loopDone:
		return 0, errWALClosed
	}
	res := <-done
	return res.lsn, res.err
}

// commitLoop — фоновая горутина, которая собирает записи в пачки
// и делает один fsync на пачку.
//
// Цикл работы:
//
//   ┌───────────────────────────────────────────────────┐
//   │                                                   │
//   │  1. Жду первую запись из pending (блокирующее)    │
//   │                                                   │
//   │  2. Забираю всё остальное, что накопилось         │
//   │     (неблокирующее, до maxBatchSize)              │
//   │                                                   │
//   │  3. Или: ticker 2ms сработал → flush что есть     │
//   │                                                   │
//   │  4. flushBatch:                                   │
//   │     - write(record) для каждой записи             │
//   │     - один Flush()                                │
//   │     - один Sync()   ← ОДИН fsync на весь батч     │
//   │     - уведомляю каждого писателя через done       │
//   │                                                   │
//   └──────────────────────┬────────────────────────────┘
//                          │
//                     повторяем
//
func (gc *GroupCommitWAL) commitLoop() {
	defer close(gc.loopDone)

	// Ticker срабатывает каждые 2 мс — это верхняя граница ожидания.
	// Если записи приходят быстрее — батч flush'ится раньше
	// (как только мы забрали всё из канала).
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	var batch []pendingWrite

	for {
		select {
		case <-gc.stop:
			// Завершение: забираем всё оставшееся в канале
			// и делаем финальный flush, чтобы не потерять данные,
			// которые уже попали в pending.
			for len(gc.pending) > 0 {
				batch = append(batch, <-gc.pending)
			}
			if len(batch) > 0 {
				gc.flushBatch(batch)
			}
			return
		case pw := <-gc.pending:
			// Получили первую запись. Теперь забираем всё,
			// что накопилось в канале за время, пока мы обрабатывали
			// предыдущий батч. Неблокирующее чтение: если канал пуст —
			// выходим из цикла сразу.
			//
			// Лимит maxBatchSize: без него при непрерывном потоке
			// записей цикл никогда бы не дошёл до fsync.
			batch = append(batch, pw)
			for len(batch) < maxBatchSize && len(gc.pending) > 0 {
				batch = append(batch, <-gc.pending)
			}
		case <-ticker.C:
			// Прошло 2 мс — если в батче что-то есть, flush'им.
			// Это гарантирует, что одинокая запись не будет ждать
			// вечно (если новых записей не поступает).
			if len(batch) == 0 {
				continue
			}
		}

		if len(batch) == 0 {
			continue
		}

		gc.flushBatch(batch)
		batch = batch[:0] // сбрасываем длину, сохраняем для переиспользования
	}
}

// flushBatch записывает весь батч и делает один fsync.
//
//   Пример: батч из 5 записей
//
//   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
//   │ rec #1  │ │ rec #2  │ │ rec #3  │ │ rec #4  │ │ rec #5  │
//   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
//        │           │           │           │           │
//        └───────────┴───────────┴───────────┴───────────┘
//                                │
//                          Write (×5)
//                                │
//                           Flush (×1)
//                                │
//                           Sync  (×1)  ← ОДИН fsync вместо пяти
//                                │
//        ┌───────────┬───────────┬───────────┬───────────┐
//        ▼           ▼           ▼           ▼           ▼
//   done: LSN=6  done: LSN=7  done: LSN=8  done: LSN=9  done: LSN=10
//
// LSN назначается здесь, в порядке записи — это гарантирует, что
// порядок LSN совпадает с физическим порядком записей в файле.
func (gc *GroupCommitWAL) flushBatch(batch []pendingWrite) {
	var writeErr error

	// Записываем все записи в bufio-буфер.
	for _, pw := range batch {
		if _, err := gc.writer.Write(pw.record); err != nil {
			writeErr = err
			break
		}
	}

	// Один Flush: bufio → OS page cache.
	if writeErr == nil {
		writeErr = gc.writer.Flush()
	}

	// Один Sync (fsync): OS page cache → физический диск.
	// Это единственный fsync на весь батч — главная оптимизация.
	if writeErr == nil {
		writeErr = gc.file.Sync()
	}

	// При ошибке сбрасываем bufio-буфер: в нём могут остаться частично
	// записанные данные от этого батча. Без Reset следующий Flush
	// попытается дописать мусор в файл.
	if writeErr != nil {
		gc.writer.Reset(gc.file)
	}

	// Уведомляем каждого писателя о результате.
	// При ошибке все получат одну и ту же ошибку.
	// При успехе — каждый получит свой уникальный LSN.
	for _, pw := range batch {
		if writeErr != nil {
			pw.done <- writeResult{err: writeErr}
		} else {
			gc.lsn++
			pw.done <- writeResult{lsn: gc.lsn}
		}
	}
}

// =============================================================================
// Вспомогательные функции: формирование и разбор записей
// =============================================================================

// buildPutPayload формирует payload для Put-записи:
//
//   ┌─────────┬─────────┬─────────┬───────────┐
//   │ key_len │ val_len │   key   │   value   │
//   │  4 B    │  4 B    │  N B    │   M B     │
//   └─────────┴─────────┴─────────┴───────────┘
func buildPutPayload(key, value []byte) []byte {
	payload := make([]byte, 4+4+len(key)+len(value))
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(value)))
	copy(payload[8:], key)
	copy(payload[8+len(key):], value)
	return payload
}

// buildDeletePayload формирует payload для Delete-записи:
//
//   ┌─────────┬─────────┐
//   │ key_len │   key   │
//   │  4 B    │  N B    │
//   └─────────┴─────────┘
//
// В отличие от Put, значения нет — удаление не несёт данных.
func buildDeletePayload(key []byte) []byte {
	payload := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(key)))
	copy(payload[4:], key)
	return payload
}

// buildRecord собирает полную WAL-запись из типа и payload:
//
//   ┌──────┬───────┬──────┬──────────────────┐
//   │ CRC  │ Len   │ Type │ Payload          │
//   │ 4B   │ 4B    │ 1B   │ N bytes          │
//   └──────┴───────┴──────┴──────────────────┘
//
// CRC считается от [type || payload] — включение type в CRC защищает
// от подмены типа операции (например, Put → Delete) при повреждении.
func buildRecord(recType RecordType, payload []byte) []byte {
	// Вычисляем CRC от type + payload.
	h := crc32.NewIEEE()
	h.Write([]byte{byte(recType)})
	h.Write(payload)
	crc := h.Sum32()

	// Собираем запись: [CRC(4)] [Len(4)] [Type(1)] [Payload(N)]
	record := make([]byte, 4+4+1+len(payload))
	binary.LittleEndian.PutUint32(record[0:4], crc)
	binary.LittleEndian.PutUint32(record[4:8], uint32(len(payload)))
	record[8] = byte(recType)
	copy(record[9:], payload)
	return record
}

// parsePutPayload разбирает payload Put-записи.
//
//   payload: [key_len(4)] [val_len(4)] [key] [value]
//
// Возвращает копии key и value — независимые от исходного буфера.
// Это важно: caller может переиспользовать буфер чтения,
// и без копирования key/value указывали бы на мусор.
func parsePutPayload(payload []byte) (key, value []byte, err error) {
	if len(payload) < 8 {
		return nil, nil, errors.New("put payload too short")
	}
	keyLen := binary.LittleEndian.Uint32(payload[0:4])
	valLen := binary.LittleEndian.Uint32(payload[4:8])
	if int(keyLen)+int(valLen)+8 > len(payload) {
		return nil, nil, errors.New("put payload: key/val len exceeds payload size")
	}
	// append([]byte(nil), ...) — идиома для создания копии слайса.
	key = append([]byte(nil), payload[8:8+keyLen]...)
	value = append([]byte(nil), payload[8+keyLen:8+keyLen+valLen]...)
	return key, value, nil
}

// parseDeletePayload разбирает payload Delete-записи.
//
//   payload: [key_len(4)] [key]
//
// Возвращает копию key.
func parseDeletePayload(payload []byte) (key []byte, err error) {
	if len(payload) < 4 {
		return nil, errors.New("delete payload too short")
	}
	keyLen := binary.LittleEndian.Uint32(payload[0:4])
	if int(keyLen)+4 > len(payload) {
		return nil, errors.New("delete payload: key len exceeds payload size")
	}
	return append([]byte(nil), payload[4:4+keyLen]...), nil
}
