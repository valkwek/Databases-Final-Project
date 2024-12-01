package godb

import (
	"fmt"
	"sync"
)

// Permissions used when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type TransactionPhase int

const (
	ReadPhase TransactionPhase = iota
	ValidationPhase
	WritePhase
)

type BufferPool struct {
	pages                  map[any]Page
	transactionPages       map[TransactionID]map[any]Page
	numPages               int
	currPage               int
	mutex                  sync.Mutex
	dirtyPages             map[TransactionID][]any
	sharedPages            map[TransactionID][]any
	runningTransactions    map[TransactionID]TransactionPhase
	concurrentAccessRecord map[TransactionID]map[TransactionID]map[any]RWPerm
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		pages:                  make(map[any]Page),
		transactionPages:       make(map[TransactionID]map[any]Page),
		numPages:               numPages,
		currPage:               0,
		mutex:                  sync.Mutex{},
		dirtyPages:             make(map[TransactionID][]any),
		sharedPages:            make(map[TransactionID][]any),
		runningTransactions:    make(map[TransactionID]TransactionPhase),
		concurrentAccessRecord: make(map[TransactionID]map[TransactionID]map[any]RWPerm),
	}, nil
}

func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	for _, page := range bp.pages {
		dbfile := page.getFile()
		dbfile.(*HeapFile).flushPage(page)
		page.setDirty(0, false)
	}
	bp.pages = make(map[any]Page)
	bp.currPage = 0
}

// helper function to find in map
func (bp *BufferPool) ExistAccess(pageKey any, tid TransactionID, dirty bool) bool {
	var pages []any
	if dirty {
		pages = bp.dirtyPages[tid]
	} else {
		pages = bp.sharedPages[tid]
	}
	for _, key := range pages {
		if key == pageKey {
			return true
		}
	}
	return false
}

// Abort a transaction and clean up resources
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	// aborted transaction no longer conflicts with other concurrent transactions
	for conflictTid, accessMap := range bp.concurrentAccessRecord {
		if conflictTid == tid {
			continue
		}
		if _, exists := accessMap[tid]; exists {
			delete(bp.concurrentAccessRecord[conflictTid], tid)
		}
	}
	for _, dirtyKey := range bp.dirtyPages[tid] {
		delete(bp.pages, dirtyKey)
	}
	delete(bp.transactionPages, tid)
	delete(bp.concurrentAccessRecord, tid)
	delete(bp.dirtyPages, tid)
	delete(bp.sharedPages, tid)
	delete(bp.runningTransactions, tid)
}

// Commit a transaction with OCC validation
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.mutex.Lock()

	// Validation phase
	bp.runningTransactions[tid] = ValidationPhase
	// Check for conflicts
	for otherTid, accessMap := range bp.concurrentAccessRecord[tid] {
		if otherTid == tid {
			continue
		}
		for pageKey, accessPerm := range accessMap {
			// 1) W(Ti) ∩ R(Tj) ≠ { }, and Ti does not finish writing before Tj starts
			if accessPerm == WritePerm && bp.ExistAccess(pageKey, tid, false) {
				bp.mutex.Unlock()
				bp.AbortTransaction(tid)
				return
			}
			// W(Ti) ∩ (W(Tj) U R(Tj)) ≠ { }, and Tj overlaps with Ti validation or write phase
			if accessPerm == WritePerm && bp.ExistAccess(pageKey, tid, true) {
				bp.mutex.Unlock()
				bp.AbortTransaction(tid)
				return
			}
		}
	}

	// Write phase
	bp.runningTransactions[tid] = WritePhase
	// Commit
	for pageKey, pageCopy := range bp.transactionPages[tid] {
		bp.pages[pageKey] = pageCopy
		if pageCopy.isDirty() {
			dbfile := pageCopy.getFile()
			dbfile.(*HeapFile).flushPage(pageCopy)
			pageCopy.setDirty(0, false)
		}
	}
	// Add as concurrent access/potential conflict to all tids in concurrent access record
	for otherTid := range bp.concurrentAccessRecord {
		if otherTid != tid {
			if _, exists := bp.concurrentAccessRecord[otherTid][tid]; !exists {
				bp.concurrentAccessRecord[otherTid][tid] = make(map[any]RWPerm)
			}
			for _, pageKey := range bp.sharedPages[tid] {
				bp.concurrentAccessRecord[otherTid][tid][pageKey] = ReadPerm
			}
			for _, pageKey := range bp.dirtyPages[tid] {
				bp.concurrentAccessRecord[otherTid][tid][pageKey] = WritePerm
			}
		}
	}
	// Clear transaction records
	delete(bp.transactionPages, tid)
	delete(bp.concurrentAccessRecord, tid)
	delete(bp.dirtyPages, tid)
	delete(bp.sharedPages, tid)
	delete(bp.runningTransactions, tid)
	bp.mutex.Unlock()
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	if _, exists := bp.runningTransactions[tid]; exists {
		return fmt.Errorf("transaction is already running")
	}

	bp.runningTransactions[tid] = ReadPhase
	bp.concurrentAccessRecord[tid] = make(map[TransactionID]map[any]RWPerm)
	bp.transactionPages[tid] = make(map[any]Page)
	return nil
}

func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	pageKey := file.pageKey(pageNo)
	// Check if the transaction already has a copy of the page
	if _, exists := bp.transactionPages[tid][pageKey]; !exists {
		// If no copy exists, check if buffer pool has space + load the original page from the file
		pageKey := file.pageKey(pageNo)
		_, exists := bp.pages[pageKey]
		if !exists {
			numDirty := 0
			for _, page := range bp.pages {
				if page.isDirty() {
					numDirty += 1
				}
			}
			if numDirty == bp.numPages {
				return nil, fmt.Errorf("buffer is full of dirty pages")
			}

			page, err := file.readPage(pageNo)
			if err != nil {
				return nil, fmt.Errorf("could not read page")
			}
			if len(bp.pages) == bp.numPages {
				for _, page := range bp.pages {
					if !page.isDirty() {
						delete(bp.pages, page.getFile().pageKey(page.(*heapPage).PageNo))
						break
					}
				}
			}
			bp.pages[pageKey] = page
			bp.currPage++
		}
		originalPage := bp.pages[pageKey]

		// Create a new Page and copy the contents of the original page for writes
		hp := originalPage.(*heapPage)
		tuplesCopy := make([]*Tuple, len(hp.tuples))
		copy(tuplesCopy, hp.tuples)
		newPage := &heapPage{
			Desc:       hp.Desc,
			PageNo:     hp.PageNo,
			HeapF:      hp.HeapF,
			tuples:     tuplesCopy,
			IsDirty:    hp.IsDirty,
			numUsed:    hp.numUsed,
			numSlots:   hp.numSlots,
			emptySlots: append([]int{}, hp.emptySlots...),
		}
		if _, exists := bp.transactionPages[tid]; !exists {
			bp.transactionPages[tid] = make(map[any]Page)
		}
		bp.transactionPages[tid][pageKey] = newPage
	}

	// Record access in shared pages or dirty pages data structure
	if perm == WritePerm {
		bp.dirtyPages[tid] = append(bp.dirtyPages[tid], pageKey)
	} else {
		bp.sharedPages[tid] = append(bp.sharedPages[tid], pageKey)
	}

	return bp.transactionPages[tid][pageKey], nil
}
