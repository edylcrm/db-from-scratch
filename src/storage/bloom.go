package storage

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	bits    []uint64
	numBits uint64
	numHash int
}

// NewBloomFilter создаёт bloom filter под expectedItems элементов
// с вероятностью ложного срабатывания falsePositiveRate (например 0.01 = 1%).
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	// Формула для оптимального размера:
	// m = -(n * ln(p)) / (ln(2))^2
	m := int(math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) /
		(math.Log(2) * math.Log(2))))

	// Оптимальное число хэш-функций:
	// k = (m/n) * ln(2)
	k := int(math.Ceil(float64(m) / float64(expectedItems) * math.Log(2)))

	return &BloomFilter{
		bits:    make([]uint64, (m+63)/64),
		numBits: uint64(m),
		numHash: k,
	}
}

func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := murmur3.Sum128(key)
	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
}

func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := murmur3.Sum128(key)
	for i := 0; i < bf.numHash; i++ {
		pos := (h1 + uint64(i)*h2) % bf.numBits
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false // точно нет
		}
	}
	return true // возможно есть
}

// TODO посмотреть внимательно
func serializeBloom(bf *BloomFilter) []byte {
	// Размер: 8 (numBits) + 4 (numHash) + 4 (bits count) + 8 * len(bits)
	buf := make([]byte, 16+8*len(bf.bits))

	binary.LittleEndian.PutUint64(buf[0:8], bf.numBits)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(bf.numHash))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(bf.bits)))

	offset := 16
	for _, word := range bf.bits {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], word)
		offset += 8
	}

	return buf
}

// TODO посмотреть внимательно
func deserializeBloom(data []byte) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("bloom filter data too short: %d bytes", len(data))
	}

	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHash := binary.LittleEndian.Uint32(data[8:12])
	bitsCount := binary.LittleEndian.Uint32(data[12:16])

	expectedSize := 16 + 8*int(bitsCount)
	if len(data) < expectedSize {
		return nil, fmt.Errorf("bloom filter data truncated: got %d, want %d",
			len(data), expectedSize)
	}

	bits := make([]uint64, bitsCount)
	offset := 16
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
	}

	return &BloomFilter{
		bits:    bits,
		numBits: numBits,
		numHash: int(numHash),
	}, nil
}
