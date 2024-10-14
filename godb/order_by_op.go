package godb

import (
	"sort"
)

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	// TODO: You may want to add additional fields here
	ascending []bool
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending}, nil //replace me
}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor() // replace me
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	var tuples []*Tuple
	for {
		tuple, err := childIter()
		if err != nil {
			return nil, err
		}
		if tuple == nil {
			break
		}
		tuples = append(tuples, tuple)
	}
	sort.Sort(tupleSorter{tuples: tuples, orderBy: o.orderBy, ascending: o.ascending})
	curr := 0
	return func() (*Tuple, error) {
		if curr == len(tuples) {
			return nil, nil
		}
		curr += 1
		return tuples[curr-1], nil
	}, nil // replace me
}

type tupleSorter struct {
	tuples    []*Tuple
	orderBy   []Expr
	ascending []bool
}

func (ts tupleSorter) Len() int {
	return len(ts.tuples)
}

func (ts tupleSorter) Swap(i, j int) {
	ts.tuples[i], ts.tuples[j] = ts.tuples[j], ts.tuples[i]
}

func (ts tupleSorter) Less(i, j int) bool {
	for index, expr := range ts.orderBy {
		tupleI, _ := expr.EvalExpr(ts.tuples[i])
		tupleJ, _ := expr.EvalExpr(ts.tuples[j])
		if tupleI.EvalPred(tupleJ, OpEq) {
			continue
		}
		if ts.ascending[index] {
			return tupleI.EvalPred(tupleJ, OpLt)
		}
		return !tupleI.EvalPred(tupleJ, OpLt)
	}
	return false
}
