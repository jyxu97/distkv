// Package storage - Memory monitoring and management
// Provides memory usage tracking and enforcement of limits
package storage

import (
	"distkv/pkg/logging"
	"distkv/pkg/metrics"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryMonitor tracks and manages memory usage for the storage engine.
type MemoryMonitor struct {
	// config holds memory limits and monitoring settings
	config *MemoryConfig

	// currentUsage tracks current memory usage in bytes
	currentUsage atomic.Int64

	// memTableUsage tracks memory used by MemTables
	memTableUsage atomic.Int64

	// cacheUsage tracks memory used by caches
	cacheUsage atomic.Int64

	// mutex protects state changes
	mutex sync.RWMutex

	// stopChan signals shutdown
	stopChan chan struct{}

	// wg tracks background goroutines
	wg sync.WaitGroup

	// logger is the component logger
	logger *logging.Logger

	// metrics collector
	metrics *metrics.MetricsCollector

	// callbacks for memory pressure events
	pressureCallbacks []MemoryPressureCallback
}

// MemoryConfig holds configuration for memory management.
type MemoryConfig struct {
	// MaxMemoryUsage is the maximum total memory usage in bytes (0 = unlimited)
	MaxMemoryUsage int64

	// MaxMemTableMemory is the maximum memory for all MemTables (0 = unlimited)
	MaxMemTableMemory int64

	// MaxCacheMemory is the maximum memory for caches (0 = unlimited)
	MaxCacheMemory int64

	// MemoryPressureThreshold triggers callbacks (0.0-1.0, e.g., 0.9 = 90%)
	MemoryPressureThreshold float64

	// MonitoringInterval is how often to check memory usage
	MonitoringInterval time.Duration

	// EnableGCTuning enables automatic GC tuning based on memory pressure
	EnableGCTuning bool
}

// MemoryPressureLevel indicates the severity of memory pressure.
type MemoryPressureLevel int

const (
	MemoryPressureNone MemoryPressureLevel = iota
	MemoryPressureLow
	MemoryPressureMedium
	MemoryPressureHigh
	MemoryPressureCritical
)

// MemoryPressureCallback is called when memory pressure changes.
type MemoryPressureCallback func(level MemoryPressureLevel)

// DefaultMemoryConfig returns reasonable default memory configuration.
func DefaultMemoryConfig() *MemoryConfig {
	return &MemoryConfig{
		MaxMemoryUsage:          2 * 1024 * 1024 * 1024, // 2GB default
		MaxMemTableMemory:       512 * 1024 * 1024,      // 512MB for MemTables
		MaxCacheMemory:          512 * 1024 * 1024,      // 512MB for caches
		MemoryPressureThreshold: 0.85,                   // 85%
		MonitoringInterval:      5 * time.Second,
		EnableGCTuning:          true,
	}
}

// NewMemoryMonitor creates a new memory monitor.
func NewMemoryMonitor(config *MemoryConfig) *MemoryMonitor {
	logger := logging.WithComponent("memory-monitor")

	if config == nil {
		config = DefaultMemoryConfig()
		logger.Info("Using default memory configuration")
	}

	monitor := &MemoryMonitor{
		config:            config,
		stopChan:          make(chan struct{}),
		logger:            logger,
		metrics:           metrics.GetGlobalMetrics(),
		pressureCallbacks: make([]MemoryPressureCallback, 0),
	}

	// Start background monitoring
	monitor.startMonitoring()

	logger.WithFields(map[string]interface{}{
		"maxMemory":         config.MaxMemoryUsage,
		"maxMemTable":       config.MaxMemTableMemory,
		"maxCache":          config.MaxCacheMemory,
		"pressureThreshold": config.MemoryPressureThreshold,
	}).Info("Memory monitor initialized")

	return monitor
}

// RecordMemTableAllocation records memory allocated for a MemTable. If that
// would exceed a limit, it records nothing and returns ErrMemoryLimitExceeded.
func (m *MemoryMonitor) RecordMemTableAllocation(bytes int64) error {
	newUsage := m.memTableUsage.Add(bytes)
	m.currentUsage.Add(bytes)

	// Check if we exceed MemTable limit
	if m.config.MaxMemTableMemory > 0 && newUsage > m.config.MaxMemTableMemory {
		m.RecordMemTableDeallocation(bytes)
		m.logger.WithFields(map[string]interface{}{
			"current": newUsage,
			"limit":   m.config.MaxMemTableMemory,
		}).Warn("MemTable memory limit exceeded")
		return ErrMemoryLimitExceeded
	}

	// Check total memory limit
	if err := m.checkTotalMemoryLimit(); err != nil {
		m.RecordMemTableDeallocation(bytes)
		return err
	}

	// Memory usage metrics are tracked via GetMemoryUsage() and Stats()

	return nil
}

// RecordMemTableDeallocation records memory freed from a MemTable.
func (m *MemoryMonitor) RecordMemTableDeallocation(bytes int64) {
	m.memTableUsage.Add(-bytes)
	m.currentUsage.Add(-bytes)
}

// RecordCacheAllocation records memory allocated for cache.
func (m *MemoryMonitor) RecordCacheAllocation(bytes int64) error {
	newUsage := m.cacheUsage.Add(bytes)
	m.currentUsage.Add(bytes)

	// Check if we exceed cache limit
	if m.config.MaxCacheMemory > 0 && newUsage > m.config.MaxCacheMemory {
		m.logger.WithFields(map[string]interface{}{
			"current": newUsage,
			"limit":   m.config.MaxCacheMemory,
		}).Warn("Cache memory limit exceeded")
		return ErrMemoryLimitExceeded
	}

	// Check total memory limit
	if err := m.checkTotalMemoryLimit(); err != nil {
		return err
	}

	return nil
}

// RecordCacheDeallocation records memory freed from cache.
func (m *MemoryMonitor) RecordCacheDeallocation(bytes int64) {
	m.cacheUsage.Add(-bytes)
	m.currentUsage.Add(-bytes)
}

// checkTotalMemoryLimit checks if total memory usage exceeds limit.
func (m *MemoryMonitor) checkTotalMemoryLimit() error {
	if m.config.MaxMemoryUsage <= 0 {
		return nil
	}

	currentUsage := m.currentUsage.Load()
	if currentUsage > m.config.MaxMemoryUsage {
		m.logger.WithFields(map[string]interface{}{
			"current": currentUsage,
			"limit":   m.config.MaxMemoryUsage,
		}).Error("Total memory limit exceeded")
		return ErrMemoryLimitExceeded
	}

	return nil
}

// GetMemoryUsage returns current memory usage statistics.
func (m *MemoryMonitor) GetMemoryUsage() MemoryUsageStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return MemoryUsageStats{
		TotalUsage:    m.currentUsage.Load(),
		MemTableUsage: m.memTableUsage.Load(),
		CacheUsage:    m.cacheUsage.Load(),
		SystemAlloc:   int64(memStats.Alloc),
		SystemTotal:   int64(memStats.TotalAlloc),
		SystemSys:     int64(memStats.Sys),
		HeapAlloc:     int64(memStats.HeapAlloc),
		HeapInuse:     int64(memStats.HeapInuse),
		NumGC:         memStats.NumGC,
	}
}

// GetPressureLevel returns current memory pressure level.
func (m *MemoryMonitor) GetPressureLevel() MemoryPressureLevel {
	if m.config.MaxMemoryUsage <= 0 {
		return MemoryPressureNone
	}

	usage := float64(m.currentUsage.Load()) / float64(m.config.MaxMemoryUsage)

	switch {
	case usage < 0.7:
		return MemoryPressureNone
	case usage < 0.8:
		return MemoryPressureLow
	case usage < 0.9:
		return MemoryPressureMedium
	case usage < 0.95:
		return MemoryPressureHigh
	default:
		return MemoryPressureCritical
	}
}

// RegisterPressureCallback registers a callback for memory pressure events.
func (m *MemoryMonitor) RegisterPressureCallback(callback MemoryPressureCallback) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.pressureCallbacks = append(m.pressureCallbacks, callback)
}

// startMonitoring starts background memory monitoring.
func (m *MemoryMonitor) startMonitoring() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.config.MonitoringInterval)
		defer ticker.Stop()

		lastPressureLevel := MemoryPressureNone

		for {
			select {
			case <-ticker.C:
				m.checkMemoryPressure(&lastPressureLevel)
			case <-m.stopChan:
				return
			}
		}
	}()
}

// checkMemoryPressure checks current memory pressure and triggers callbacks.
func (m *MemoryMonitor) checkMemoryPressure(lastLevel *MemoryPressureLevel) {
	currentLevel := m.GetPressureLevel()

	// Log if pressure level changed
	if currentLevel != *lastLevel {
		usage := m.GetMemoryUsage()
		m.logger.WithFields(map[string]interface{}{
			"level":         currentLevel,
			"totalUsage":    usage.TotalUsage,
			"memTableUsage": usage.MemTableUsage,
			"cacheUsage":    usage.CacheUsage,
			"heapAlloc":     usage.HeapAlloc,
		}).Info("Memory pressure level changed")

		// Notify callbacks
		m.mutex.RLock()
		callbacks := make([]MemoryPressureCallback, len(m.pressureCallbacks))
		copy(callbacks, m.pressureCallbacks)
		m.mutex.RUnlock()

		for _, callback := range callbacks {
			go callback(currentLevel)
		}

		*lastLevel = currentLevel
	}

	// Auto-tune GC if enabled
	if m.config.EnableGCTuning && currentLevel >= MemoryPressureHigh {
		runtime.GC()
		m.logger.Debug("Triggered garbage collection due to high memory pressure")
	}
}

// Stop stops the memory monitor.
func (m *MemoryMonitor) Stop() {
	m.logger.Info("Stopping memory monitor")
	close(m.stopChan)
	m.wg.Wait()
	m.logger.Info("Memory monitor stopped")
}

// MemoryUsageStats provides detailed memory usage information.
type MemoryUsageStats struct {
	TotalUsage    int64  // Total tracked usage
	MemTableUsage int64  // Memory used by MemTables
	CacheUsage    int64  // Memory used by caches
	SystemAlloc   int64  // Bytes allocated by Go runtime
	SystemTotal   int64  // Total bytes allocated over time
	SystemSys     int64  // Bytes obtained from system
	HeapAlloc     int64  // Bytes allocated on heap
	HeapInuse     int64  // Bytes in use on heap
	NumGC         uint32 // Number of GC runs
}
