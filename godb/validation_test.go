package godb

import (
	"testing"
)

/**
* Test to construct an invalidation situation.
* tid1 writes t1 to page; tid2 writes t2 to same page; tid1 tries to commit; tid2 tries to commit
* outcome: tid1 commits, tid2 aborts
 */
func TestInvalidateWriteWrite(t *testing.T) {
	_, t2, t1, _, _, _ := makeTestVars(t)
	bp, hf, tid1, tid2, t2 := transactionTestSetUp(t)

	pg, _ := bp.GetPage(hf, 2, tid1, WritePerm)
	heapp := pg.(*heapPage)
	heapp.insertTuple(&t1)
	heapp.setDirty(tid1, true)

	pg2, _ := bp.GetPage(hf, 2, tid2, WritePerm)
	heapp2 := pg2.(*heapPage)
	heapp2.insertTuple(&t2)
	heapp2.setDirty(tid2, true)

	bp.CommitTransaction(tid1)
	bp.CommitTransaction(tid2)

	bp.FlushAllPages()

	pg, _ = bp.GetPage(hf, 2, tid1, WritePerm)
	heapp = pg.(*heapPage)
	iter := heapp.tupleIter()

	correctCommit := false
	correctAbort := true
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t1.equals(tup) {
			correctCommit = correctCommit || true
		}
		if t2.equals(tup) {
			correctAbort = false
		}
	}

	if !correctCommit || !correctAbort {
		t.Errorf("Commit: %t, Abort: %t", correctCommit, correctAbort)
	}
}

/**
* Test to construct an invalidation situation.
* tid1 writes t1 to page; tid2 reads from same page + writes t2 to different page; tid1 tries to commit; tid2 tries to commit
* outcome: tid1 commits, tid2 aborts
 */
func TestInvalidateWriteRead(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars(t)
	bp, hf, tid1, tid2, t1 := transactionTestSetUp(t)

	pg, _ := bp.GetPage(hf, 2, tid1, WritePerm)
	heapp := pg.(*heapPage)
	heapp.insertTuple(&t1)
	heapp.setDirty(tid1, true)

	bp.GetPage(hf, 2, tid2, ReadPerm)
	pg2, _ := bp.GetPage(hf, 1, tid2, WritePerm)
	heapp2 := pg2.(*heapPage)
	heapp2.insertTuple(&t2)
	heapp2.setDirty(tid2, true)

	bp.CommitTransaction(tid1)
	bp.CommitTransaction(tid2)

	bp.FlushAllPages()

	pg, _ = bp.GetPage(hf, 2, tid1, WritePerm)
	heapp = pg.(*heapPage)
	iter := heapp.tupleIter()

	correctCommit := false
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t1.equals(tup) {
			correctCommit = correctCommit || true
		}
	}

	pg, _ = bp.GetPage(hf, 1, tid1, WritePerm)
	heapp = pg.(*heapPage)
	iter = heapp.tupleIter()

	correctAbort := false
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t2.equals(tup) {
			correctAbort = false
		}
	}

	if !correctCommit {
		t.Errorf("Commit: %t, Abort: %t", correctCommit, correctAbort)
	}
}

/**
* Test to construct a validation situation.
* tid1 reads from page + writes t1 to different page; tid2 writes t2 to same page that tid1 read from; tid1 tries to commit; tid2 tries to commit
* outcome: tid1 commits, tid2 commits
 */
func TestValidateReadWrite(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars(t)
	bp, hf, tid1, tid2, t1 := transactionTestSetUp(t)

	bp.GetPage(hf, 2, tid1, ReadPerm)
	pg, _ := bp.GetPage(hf, 1, tid1, WritePerm)
	heapp := pg.(*heapPage)
	heapp.insertTuple(&t1)
	heapp.setDirty(tid1, true)

	pg2, _ := bp.GetPage(hf, 1, tid2, WritePerm)
	heapp2 := pg2.(*heapPage)
	heapp2.insertTuple(&t2)
	heapp2.setDirty(tid2, true)

	bp.CommitTransaction(tid1)
	bp.CommitTransaction(tid2)

	bp.FlushAllPages()

	pg, _ = bp.GetPage(hf, 1, tid1, WritePerm)
	heapp = pg.(*heapPage)
	iter := heapp.tupleIter()

	correctCommit1 := false
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t1.equals(tup) {
			correctCommit1 = correctCommit1 || true
		}
	}

	pg, _ = bp.GetPage(hf, 2, tid1, WritePerm)
	heapp = pg.(*heapPage)
	iter = heapp.tupleIter()

	correctCommit2 := false
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t2.equals(tup) {
			correctCommit2 = correctCommit2 || true
		}
	}

	if !correctCommit1 || !correctCommit2 {
		t.Errorf("Commit 1: %t, Commit 2: %t", correctCommit1, correctCommit2)
	}
}

/**
* Test to construct a validation situation.
* tid1 reads from page + writes t1 to different page; tid2 reads from same page tid1 read from + writes t2 to different page than what tid1 read and wrote to; tid1 tries to commit; tid2 tries to commit
* outcome: tid1 commits, tid2 commits
 */
func TestValidateReadRead(t *testing.T) {
	_, t1, t2, _, _, _ := makeTestVars(t)
	bp, hf, tid1, tid2, t1 := transactionTestSetUp(t)

	bp.GetPage(hf, 2, tid1, WritePerm)
	pg, _ := bp.GetPage(hf, 1, tid1, WritePerm)
	heapp := pg.(*heapPage)
	heapp.insertTuple(&t1)
	heapp.setDirty(tid1, true)

	bp.GetPage(hf, 2, tid2, ReadPerm)
	pg2, _ := bp.GetPage(hf, 0, tid2, WritePerm)
	heapp2 := pg2.(*heapPage)
	heapp2.insertTuple(&t2)
	heapp2.setDirty(tid2, true)

	bp.CommitTransaction(tid1)
	bp.CommitTransaction(tid2)

	bp.FlushAllPages()

	pg, _ = bp.GetPage(hf, 1, tid1, WritePerm)
	heapp = pg.(*heapPage)
	iter := heapp.tupleIter()

	correctCommit1 := false
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t1.equals(tup) {
			correctCommit1 = correctCommit1 || true
		}
	}

	pg, _ = bp.GetPage(hf, 0, tid1, WritePerm)
	heapp = pg.(*heapPage)
	iter = heapp.tupleIter()

	correctCommit2 := false
	for tup, err := iter(); tup != nil || err != nil; tup, err = iter() {
		if err != nil {
			t.Fatalf("Iterator error")
		}
		if t2.equals(tup) {
			correctCommit2 = correctCommit2 || true
		}
	}

	if !correctCommit1 || !correctCommit2 {
		t.Errorf("Commit 1: %t, Commit 2: %t", correctCommit1, correctCommit2)
	}
}

/**
* Stress testing for 8 threads
 */

func TestTransactionEightThreads(t *testing.T) {
	validateTransactions(t, 8)
}
