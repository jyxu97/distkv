package storage

// waitForFlushes waits until the flush worker has flushed every sealed
// MemTable, through any retries.
func waitForFlushes(e *Engine) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	for len(e.flushingMemTables) > 0 {
		e.flushCond.Wait()
	}
}

// flushNow seals the active MemTable, whatever its size, and waits until it is
// flushed.
func flushNow(e *Engine) {
	e.mutex.Lock()
	if e.activeMemTable.Size() > 0 {
		e.sealLocked()
	}
	e.mutex.Unlock()
	waitForFlushes(e)
}
