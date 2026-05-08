// Package storage - Bloom Filter implementation
// Bloom filters provide fast probabilistic membership testing for SSTables.
// They help avoid expensive disk reads for keys that definitely don't exist.
package storage

import (
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure
// that tells us if a key "definitely doesn't exist" or "might exist".
type BloomFilter struct {
	// bitArray stores the actual bits (true/false for each position)
	bitArray []bool

	// size is the number of bits in the filter
	size uint

	// hashFunctions is the number of hash functions to use
	hashFunctions uint

	// bitsPerKey affects the false positive rate
	bitsPerKey uint
}

// NewBloomFilter creates a new bloom filter optimized for the expected number of keys.
// expectedKeys: how many keys we expect to store
// bitsPerKey: how many bits to use per key (more bits = fewer false positives)
func NewBloomFilter(expectedKeys int, bitsPerKey int) *BloomFilter {
	if expectedKeys <= 0 {
		expectedKeys = 1000 // Default assumption
	}
	if bitsPerKey <= 0 {
		bitsPerKey = 10 // Default from our config
	}

	// Calculate optimal filter size
	size := uint(expectedKeys * bitsPerKey)

	// Calculate optimal number of hash functions
	// Formula: k = (m/n) * ln(2), where m=bits, n=keys
	hashFunctions := uint(float64(bitsPerKey) * math.Ln2)
	if hashFunctions == 0 {
		hashFunctions = 1
	}
	if hashFunctions > 10 {
		hashFunctions = 10 // Cap at 10 for performance
	}

	return &BloomFilter{
		bitArray:      make([]bool, size),
		size:          size,
		hashFunctions: hashFunctions,
		bitsPerKey:    uint(bitsPerKey),
	}
}

// NewBloomFilterWithFPR creates a bloom filter optimized for a target false positive rate.
// expectedKeys: how many keys we expect to store
// targetFPR: target false positive rate (e.g., 0.01 for 1%)
func NewBloomFilterWithFPR(expectedKeys int, targetFPR float64) *BloomFilter {
	if expectedKeys <= 0 {
		expectedKeys = 1000
	}
	if targetFPR <= 0 || targetFPR >= 1 {
		targetFPR = 0.01 // Default 1% false positive rate
	}

	// Calculate optimal number of bits
	// Formula: m = -n * ln(p) / (ln(2))^2
	// where m=bits, n=keys, p=target false positive rate
	ln2Squared := math.Ln2 * math.Ln2
	optimalBits := -float64(expectedKeys) * math.Log(targetFPR) / ln2Squared
	size := uint(math.Ceil(optimalBits))

	// Calculate optimal number of hash functions
	// Formula: k = (m/n) * ln(2)
	optimalHashFunctions := (float64(size) / float64(expectedKeys)) * math.Ln2
	hashFunctions := uint(math.Ceil(optimalHashFunctions))

	// Cap hash functions for performance
	if hashFunctions == 0 {
		hashFunctions = 1
	}
	if hashFunctions > 10 {
		hashFunctions = 10
	}

	// Calculate effective bits per key
	bitsPerKey := uint(size / uint(expectedKeys))
	if bitsPerKey == 0 {
		bitsPerKey = 1
	}

	return &BloomFilter{
		bitArray:      make([]bool, size),
		size:          size,
		hashFunctions: hashFunctions,
		bitsPerKey:    bitsPerKey,
	}
}

// Add inserts a key into the bloom filter.
// This sets multiple bits based on different hash functions.
func (bf *BloomFilter) Add(key string) {
	for i := uint(0); i < bf.hashFunctions; i++ {
		// Generate hash value using the key and hash function index
		hashValue := bf.hash(key, i)

		// Set the corresponding bit
		bitIndex := hashValue % bf.size
		bf.bitArray[bitIndex] = true
	}
}

// MightContain checks if a key might be in the set.
// Returns:
//   - false: key is DEFINITELY NOT in the set
//   - true: key MIGHT be in the set (could be false positive)
func (bf *BloomFilter) MightContain(key string) bool {
	for i := uint(0); i < bf.hashFunctions; i++ {
		// Generate same hash value as when adding
		hashValue := bf.hash(key, i)

		// Check if the corresponding bit is set
		bitIndex := hashValue % bf.size
		if !bf.bitArray[bitIndex] {
			// If any bit is not set, key definitely doesn't exist
			return false
		}
	}

	// All bits were set, so key might exist (but could be false positive)
	return true
}

// hash generates a hash value for a key using a specific hash function index.
// We use FNV hash with different seeds to simulate multiple hash functions.
func (bf *BloomFilter) hash(key string, hashIndex uint) uint {
	hasher := fnv.New32a()

	// Use hash function index as a seed to get different hash values
	seedBytes := []byte{byte(hashIndex), byte(hashIndex >> 8), byte(hashIndex >> 16), byte(hashIndex >> 24)}
	hasher.Write(seedBytes)
	hasher.Write([]byte(key))

	return uint(hasher.Sum32())
}

// Serialize converts the bloom filter to bytes for storage.
// This is used when writing SSTable metadata to disk.
func (bf *BloomFilter) Serialize() []byte {
	// Calculate number of bytes needed
	byteCount := (bf.size + 7) / 8 // Round up to nearest byte

	// Create byte array
	data := make([]byte, byteCount+8) // +8 for header info

	// Write header: size (4 bytes) + hash functions (4 bytes)
	data[0] = byte(bf.size)
	data[1] = byte(bf.size >> 8)
	data[2] = byte(bf.size >> 16)
	data[3] = byte(bf.size >> 24)
	data[4] = byte(bf.hashFunctions)
	data[5] = byte(bf.hashFunctions >> 8)
	data[6] = byte(bf.hashFunctions >> 16)
	data[7] = byte(bf.hashFunctions >> 24)

	// Pack bits into bytes
	for i := uint(0); i < bf.size; i++ {
		if bf.bitArray[i] {
			byteIndex := i / 8
			bitIndex := i % 8
			data[8+byteIndex] |= (1 << bitIndex)
		}
	}

	return data
}

// DeserializeBloomFilter recreates a bloom filter from serialized bytes.
func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, ErrSSTableCorrupted
	}

	// Read header
	size := uint(data[0]) | uint(data[1])<<8 | uint(data[2])<<16 | uint(data[3])<<24
	hashFunctions := uint(data[4]) | uint(data[5])<<8 | uint(data[6])<<16 | uint(data[7])<<24

	// Calculate expected data size
	expectedByteCount := (size + 7) / 8
	if len(data) < int(8+expectedByteCount) {
		return nil, ErrSSTableCorrupted
	}

	// Create bloom filter
	bf := &BloomFilter{
		bitArray:      make([]bool, size),
		size:          size,
		hashFunctions: hashFunctions,
		bitsPerKey:    10, // Default, actual value doesn't matter for deserialized filters
	}

	// Unpack bits from bytes
	for i := uint(0); i < size; i++ {
		byteIndex := i / 8
		bitIndex := i % 8
		if data[8+byteIndex]&(1<<bitIndex) != 0 {
			bf.bitArray[i] = true
		}
	}

	return bf, nil
}

// Size returns the size of the bit array.
func (bf *BloomFilter) Size() uint {
	return bf.size
}

