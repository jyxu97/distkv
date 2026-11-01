// Package storage - SSTable (Sorted String Table) implementation
// SSTables are immutable disk files that store key-value pairs in sorted order.
// They provide the persistent storage layer of our LSM-tree.
package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// SSTable represents an immutable sorted file on disk.
// Once created, SSTables are never modified - only read or deleted.
type SSTable struct {
	// filePath is the path to the SSTable file on disk
	filePath string

	// index maps keys to their byte offsets in the file for fast lookups
	// This is loaded into memory when the SSTable is opened
	index map[string]int64

	// bloomFilter helps avoid disk reads for keys that don't exist
	bloomFilter *BloomFilter

	// minKey and maxKey define the key range in this SSTable
	minKey, maxKey string

	// size is the file size in bytes
	size int64

	// level indicates which compaction level this SSTable belongs to (0 = newest)
	level int

	// gen orders tables by how recent their data is: a flushed table gets the
	// generation of its MemTable, a compaction output that of its newest
	// input. Within a level a higher generation holds newer versions.
	gen uint64

	// mutex protects concurrent access to the SSTable
	mutex sync.RWMutex

	// file handle for reading data (kept open for performance)
	file *os.File
}

// SSTableMetadata stores information about an SSTable that's written to disk.
// This includes the index and bloom filter for fast startup.
type SSTableMetadata struct {
	Index       map[string]int64 `json:"index"`
	BloomFilter []byte           `json:"bloom_filter"`
	MinKey      string           `json:"min_key"`
	MaxKey      string           `json:"max_key"`
	EntryCount  int              `json:"entry_count"`
	FileSize    int64            `json:"file_size"`
	Level       int              `json:"level"`
	Checksum    uint32           `json:"checksum"`
}

// CreateSSTable creates a new SSTable from a MemTable by flushing to disk.
// This is where the LSM-tree write path goes from memory to persistent storage.
func CreateSSTable(memTable *MemTable, filePath string, config *StorageConfig) (*SSTable, error) {
	// Create the directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %v", err)
	}

	// Create the SSTable file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %v", err)
	}
	defer file.Close()

	// Create a buffered writer for better performance
	writer := bufio.NewWriter(file)

	// Initialize metadata
	index := make(map[string]int64)

	// Create bloom filter - use FPR if configured, otherwise use bits per key
	var bloomFilter *BloomFilter
	if config.BloomFilterFPR > 0 && config.BloomFilterFPR < 1 {
		bloomFilter = NewBloomFilterWithFPR(memTable.Count(), config.BloomFilterFPR)
	} else {
		bloomFilter = NewBloomFilter(memTable.Count(), config.BloomFilterBits)
	}

	var minKey, maxKey string
	var entryCount int
	var filePosition int64 = 0 // Track file position manually
	checksum := crc32.NewIEEE()

	// Write entries from MemTable to SSTable
	iterator := memTable.Iterator()
	defer iterator.Close()

	for iterator.Valid() {
		entry := iterator.Value()

		// Track min/max keys
		if minKey == "" || entry.Key < minKey {
			minKey = entry.Key
		}
		if maxKey == "" || entry.Key > maxKey {
			maxKey = entry.Key
		}

		// Record current file position for index
		index[entry.Key] = filePosition

		// Add key to bloom filter
		bloomFilter.Add(entry.Key)

		// Serialize and write entry
		entryData, err := serializeEntry(entry)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize entry: %v", err)
		}

		// Write entry size first (for easier reading)
		sizeBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(sizeBytes, uint32(len(entryData)))

		// Write to file and update checksum
		if _, err := writer.Write(sizeBytes); err != nil {
			return nil, fmt.Errorf("failed to write entry size: %w", err)
		}
		if _, err := writer.Write(entryData); err != nil {
			return nil, fmt.Errorf("failed to write entry data: %w", err)
		}
		checksum.Write(sizeBytes)
		checksum.Write(entryData)

		// Update file position manually
		filePosition += 4 + int64(len(entryData)) // 4 bytes for size + entry data

		entryCount++
		iterator.Next()
	}

	// Flush all data to disk. Fsync too: callers delete the WAL segment that
	// covered this data once the SSTable exists, so it must survive power
	// loss, not just a process crash.
	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush SSTable: %v", err)
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("failed to fsync SSTable: %v", err)
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %v", err)
	}

	// Create metadata
	metadata := &SSTableMetadata{
		Index:       index,
		BloomFilter: bloomFilter.Serialize(),
		MinKey:      minKey,
		MaxKey:      maxKey,
		EntryCount:  entryCount,
		FileSize:    fileInfo.Size(),
		Checksum:    checksum.Sum32(),
	}

	// Write metadata file
	metadataPath := filePath + ".meta"
	if err := writeMetadata(metadata, metadataPath); err != nil {
		return nil, fmt.Errorf("failed to write metadata: %v", err)
	}

	// Make both new files' directory entries durable
	if err := syncDir(filepath.Dir(filePath)); err != nil {
		return nil, err
	}

	// Create and return SSTable
	return OpenSSTable(filePath)
}

// OpenSSTable opens an existing SSTable from disk.
func OpenSSTable(filePath string) (*SSTable, error) {
	// Read metadata
	metadataPath := filePath + ".meta"
	metadata, err := readMetadata(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %v", err)
	}

	// Open the data file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %v", err)
	}

	// Recreate bloom filter
	bloomFilter, err := DeserializeBloomFilter(metadata.BloomFilter)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to deserialize bloom filter: %v", err)
	}

	return &SSTable{
		filePath:    filePath,
		index:       metadata.Index,
		bloomFilter: bloomFilter,
		minKey:      metadata.MinKey,
		maxKey:      metadata.MaxKey,
		size:        metadata.FileSize,
		file:        file,
	}, nil
}

// Get retrieves an entry from the SSTable by key.
func (sst *SSTable) Get(key string) (*Entry, error) {
	sst.mutex.RLock()
	defer sst.mutex.RUnlock()

	// First check bloom filter to avoid unnecessary disk reads
	if !sst.bloomFilter.MightContain(key) {
		return nil, ErrKeyNotFound
	}

	// Check if key is in our range
	if key < sst.minKey || key > sst.maxKey {
		return nil, ErrKeyNotFound
	}

	// Look up key in index
	offset, exists := sst.index[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	// Read with positional reads: callers share the file handle under a read
	// lock, so Seek followed by Read would race on the file offset and return
	// bytes from another entry.
	sizeBytes := make([]byte, 4)
	if _, err := sst.file.ReadAt(sizeBytes, offset); err != nil {
		return nil, fmt.Errorf("failed to read entry size: %v", err)
	}
	entrySize := binary.LittleEndian.Uint32(sizeBytes)

	// Read entry data
	entryData := make([]byte, entrySize)
	if _, err := sst.file.ReadAt(entryData, offset+4); err != nil {
		return nil, fmt.Errorf("failed to read entry data: %v", err)
	}

	// Deserialize entry
	entry, err := deserializeEntry(entryData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize entry: %v", err)
	}

	return entry, nil
}

// Iterator returns an iterator to scan all entries in the SSTable.
func (sst *SSTable) Iterator() (Iterator, error) {
	return NewSSTableIterator(sst), nil
}

// Close closes the SSTable file handle.
func (sst *SSTable) Close() error {
	sst.mutex.Lock()
	defer sst.mutex.Unlock()

	if sst.file != nil {
		err := sst.file.Close()
		sst.file = nil
		return err
	}
	return nil
}

// Size returns the file size in bytes.
func (sst *SSTable) Size() int64 {
	return sst.size
}

// KeyRange returns the min and max keys in this SSTable.
func (sst *SSTable) KeyRange() (string, string) {
	return sst.minKey, sst.maxKey
}

// Helper functions for serialization

func serializeEntry(entry *Entry) ([]byte, error) {
	// Simple JSON serialization for this implementation
	// Production code might use more efficient binary formats
	return json.Marshal(entry)
}

func deserializeEntry(data []byte) (*Entry, error) {
	var entry Entry
	err := json.Unmarshal(data, &entry)
	return &entry, err
}

func writeMetadata(metadata *SSTableMetadata, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty print for debugging
	if err := encoder.Encode(metadata); err != nil {
		return err
	}
	return file.Sync()
}

func readMetadata(filePath string) (*SSTableMetadata, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var metadata SSTableMetadata
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&metadata)
	return &metadata, err
}
