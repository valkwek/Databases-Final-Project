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
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending}, nil
}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor()
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

type lessFunc func(p1, p2 *Tuple) bool

type multiSorter struct {
	tuples []Tuple
	less   []lessFunc
}

func (ms *multiSorter) Sort(tuples []Tuple) {
	ms.tuples = tuples
	sort.Sort(ms)
}

func OrderedBy(less ...lessFunc) *multiSorter {
	return &multiSorter{
		less: less,
	}
}

func (ms *multiSorter) Len() int {
	return len(ms.tuples)
}

func (ms *multiSorter) Swap(i, j int) {
	ms.tuples[i], ms.tuples[j] = ms.tuples[j], ms.tuples[i]
}

func (ms *multiSorter) Less(i, j int) bool {
	p, q := &ms.tuples[i], &ms.tuples[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		switch {
		case less(p, q):
			// p < q, so we have a decision.
			return true
		case less(q, p):
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	return ms.less[k](p, q)
}

func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	allTuples := make([]Tuple, 0)

	childIterator, err := (o.child).Iterator(tid)
	if err != nil {
		return nil, err
	}
	for {
		tuple, err := childIterator()
		if err != nil {
			break
		}

		if tuple == nil {
			break
		}
		allTuples = append(allTuples, *tuple)
	}
	sortFuncs := make([]lessFunc, 0)
	for i := 0; i < len(o.orderBy); i++ {
		fieldToSortBy := o.orderBy[i]
		ascendingSort := o.ascending[i]
		if ascendingSort {
			sortFunc := func(t1, t2 *Tuple) bool {
				for i, t1Field := range t1.Desc.Fields {
					if t1Field.Fname == fieldToSortBy.GetExprType().Fname {
						t1FieldVal := t1.Fields[i]
						for i, t2Field := range t2.Desc.Fields {
							if t2Field.Fname == fieldToSortBy.GetExprType().Fname {
								t2FieldVal := t2.Fields[i]
								return t1FieldVal.EvalPred(t2FieldVal, OpLt)
							}
						}
					}
				}
				return false
			}
			sortFuncs = append(sortFuncs, sortFunc)
		} else {
			sortFunc := func(t1, t2 *Tuple) bool {
				for i, t1Field := range t1.Desc.Fields {
					if t1Field.Fname == fieldToSortBy.GetExprType().Fname {
						t1FieldVal := t1.Fields[i]
						for i, t2Field := range t2.Desc.Fields {
							if t2Field.Fname == fieldToSortBy.GetExprType().Fname {
								t2FieldVal := t2.Fields[i]
								return t1FieldVal.EvalPred(t2FieldVal, OpGt)
							}
						}
					}
				}
				return false
			}
			sortFuncs = append(sortFuncs, sortFunc)
		}
	}
	OrderedBy(sortFuncs...).Sort(allTuples)
	i := 0
	return func() (*Tuple, error) {
		if i < len(allTuples) {
			curTuple := allTuples[i]
			i++
			return &curTuple, nil
		} else {
			return nil, nil
		}
	}, nil
}
