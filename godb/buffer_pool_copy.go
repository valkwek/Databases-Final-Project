package godb

/**

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
	"sync"
	"time"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	// TODO: some code goes here
	pages               map[any]Page
	numPages            int
	currPage            int
	mutex               sync.Mutex
	pageMutexes         map[any]*pageLock
	dirtyPages          map[TransactionID][]any
	sharedPages         map[TransactionID][]any
	dependencyGraph     map[TransactionID][]TransactionID
	runningTransactions []TransactionID
}

type pageLock struct {
	sharedLocks   int
	exclusiveLock TransactionID
	mutex         sync.Mutex
	permittedTids []TransactionID
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		pages:               make(map[any]Page),
		numPages:            numPages,
		currPage:            0,
		mutex:               sync.Mutex{},
		pageMutexes:         make(map[any]*pageLock),
		dirtyPages:          make(map[TransactionID][]any),
		sharedPages:         make(map[TransactionID][]any),
		dependencyGraph:     make(map[TransactionID][]TransactionID),
		runningTransactions: []TransactionID{},
	}, nil
}

// helper function to check if transaction id is contained in list of transaction ids
func contains(list []TransactionID, value TransactionID) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

// helper function to remove transaction id from list of transaction ids
func remove(list []TransactionID, value TransactionID) []TransactionID {
	for i, v := range list {
		if v == value {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

// helper function to detect cycle in dependency graph including tid using BFS
func (bp *BufferPool) cycle(tid TransactionID) bool {
	seen := make(map[TransactionID]bool)
	var queue []TransactionID
	queue = append(queue, tid)
	for len(queue) != 0 {
		curr := queue[0]
		queue = queue[1:]
		if seen[curr] {
			return true
		}
		seen[curr] = true
		queue = append(queue, bp.dependencyGraph[tid]...)
	}
	return false
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
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

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	for _, dirtyKey := range bp.dirtyPages[tid] {
		bp.pageMutexes[dirtyKey].exclusiveLock = 0
		bp.pageMutexes[dirtyKey].permittedTids = remove(bp.pageMutexes[dirtyKey].permittedTids, tid)
		delete(bp.pages, dirtyKey)
	}
	for _, sharedKey := range bp.sharedPages[tid] {
		bp.pageMutexes[sharedKey].sharedLocks -= 1
		bp.pageMutexes[sharedKey].permittedTids = remove(bp.pageMutexes[sharedKey].permittedTids, tid)
	}
	bp.sharedPages[tid] = []any{}
	bp.dirtyPages[tid] = []any{}
	delete(bp.dependencyGraph, tid)
	remove(bp.runningTransactions, tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	for pageKey, page := range bp.pages {
		for _, dirtyKey := range bp.dirtyPages[tid] {
			if dirtyKey == pageKey {
				dbfile := page.getFile()
				dbfile.(*HeapFile).flushPage(page)
				page.setDirty(0, false)
			}
		}
	}
	for _, dirtyKey := range bp.dirtyPages[tid] {
		bp.pageMutexes[dirtyKey].exclusiveLock = 0
		bp.pageMutexes[dirtyKey].permittedTids = remove(bp.pageMutexes[dirtyKey].permittedTids, tid)
	}
	for _, sharedKey := range bp.sharedPages[tid] {
		bp.pageMutexes[sharedKey].sharedLocks -= 1
		bp.pageMutexes[sharedKey].permittedTids = remove(bp.pageMutexes[sharedKey].permittedTids, tid)
	}
	bp.sharedPages[tid] = []any{}
	bp.dirtyPages[tid] = []any{}
	remove(bp.runningTransactions, tid)
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	for _, runningTid := range bp.runningTransactions {
		if runningTid == tid {
			return fmt.Errorf("transaction is already running")
		}
	}
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	bp.mutex.Lock()

	pageKey := file.pageKey(pageNo)

	_, exists := bp.pageMutexes[pageKey]
	if !exists {
		bp.pageMutexes[pageKey] = &pageLock{
			sharedLocks:   0,
			exclusiveLock: 0,
			mutex:         sync.Mutex{},
			permittedTids: []TransactionID{},
		}
	}

	_, inBuffer := bp.pages[pageKey]

	if inBuffer {
		pageMutex, ok := bp.pageMutexes[pageKey]
		if !ok {
			return nil, fmt.Errorf("page mutex not found")
		}
		if perm == WritePerm {
			for _, dependentTid := range pageMutex.permittedTids {
				if dependentTid != tid {
					bp.dependencyGraph[tid] = append(bp.dependencyGraph[tid], dependentTid)
				}
			}
			if bp.cycle(tid) {
				bp.mutex.Unlock()
				bp.AbortTransaction(tid)
				return nil, fmt.Errorf("aborted transaction due to deadlock")
			}
		}
		bp.mutex.Unlock()
		for {
			bp.mutex.Lock()
			if perm == ReadPerm && (pageMutex.exclusiveLock == 0 || contains(pageMutex.permittedTids, tid)) {
				pageMutex.sharedLocks += 1
				pageMutex.permittedTids = append(pageMutex.permittedTids, tid)
				bp.sharedPages[tid] = append(bp.sharedPages[tid], pageKey)
				break
			}
			if perm == WritePerm && ((pageMutex.exclusiveLock == 0 && pageMutex.sharedLocks == 0) || contains(pageMutex.permittedTids, tid)) {
				pageMutex.exclusiveLock = 1
				pageMutex.permittedTids = append(pageMutex.permittedTids, tid)
				bp.dirtyPages[tid] = append(bp.dirtyPages[tid], pageKey)
				break
			}
			bp.mutex.Unlock()
			time.Sleep(1000)
		}
		page := bp.pages[pageKey]
		bp.mutex.Unlock()
		return page, nil
	}

	numDirty := 0
	for _, page := range bp.pages {
		if page.isDirty() {
			numDirty += 1
		}
	}
	if numDirty == bp.numPages {
		bp.mutex.Unlock()
		return nil, fmt.Errorf("buffer is full of dirty pages")
	}

	page, err := file.readPage(pageNo)
	if err != nil {
		bp.mutex.Unlock()
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
	bp.pageMutexes[pageKey] = &pageLock{
		sharedLocks:   0,
		exclusiveLock: 0,
		mutex:         sync.Mutex{},
		permittedTids: []TransactionID{},
	}
	pageMutex := bp.pageMutexes[pageKey]
	if perm == ReadPerm {
		pageMutex.sharedLocks += 1
		pageMutex.permittedTids = append(pageMutex.permittedTids, tid)
		bp.sharedPages[tid] = append(bp.sharedPages[tid], pageKey)
	}
	if perm == WritePerm {
		pageMutex.exclusiveLock = 1
		pageMutex.permittedTids = append(pageMutex.permittedTids, tid)
		bp.dirtyPages[tid] = append(bp.dirtyPages[tid], pageKey)
	}
	defer bp.mutex.Unlock()
	return page, nil
}

*/
