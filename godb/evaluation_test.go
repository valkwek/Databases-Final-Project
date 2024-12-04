package godb

import (
	"sync"
	"testing"
)

func evalTransactions(t *testing.T, threads int) {
	bp, hf, _, _, _, _ := transactionTestSetUpVarLen(t, 1, 1)

	var startWg, readyWg sync.WaitGroup
	startChan := make(chan struct{})

	incrementer := func(thrId int) {
		// Signal that this goroutine is ready
		readyWg.Done()

		// Wait for the signal to start
		<-startChan

		for tid := TransactionID(0); ; {
			tid = NewTID()
			bp.BeginTransaction(tid)

			iter1, _ := hf.Iterator(tid)
			readTup, _ := iter1()

			var writeTup = Tuple{
				Desc: readTup.Desc,
				Fields: []DBValue{
					readTup.Fields[0],
					IntField{readTup.Fields[1].(IntField).Value + 1},
				}}

			dop := NewDeleteOp(hf, hf)
			iterDel, _ := dop.Iterator(tid)
			iterDel()

			iop := NewInsertOp(hf, &Singleton{writeTup, false})
			iterIns, _ := iop.Iterator(tid)
			iterIns()

			bp.CommitTransaction(tid)
			break // Exit on success
		}
		startWg.Done()
	}

	// Prepare goroutines
	readyWg.Add(threads)
	startWg.Add(threads)
	for i := 0; i < threads; i++ {
		go incrementer(i)
	}

	// Wait for all goroutines to be ready
	readyWg.Wait()

	// Start all goroutines at once
	close(startChan)

	// Wait for all goroutines to finish
	startWg.Wait()
}

func TestTransaction10000(t *testing.T) {
	evalTransactions(t, 10000)
}

func TestTransaction15000(t *testing.T) {
	evalTransactions(t, 15000)
}

func TestTransaction20000(t *testing.T) {
	evalTransactions(t, 20000)
}

func TestTransaction25000(t *testing.T) {
	evalTransactions(t, 25000)
}

func TestTransaction30000(t *testing.T) {
	evalTransactions(t, 30000)
}
