package godb

// import (
// "fmt"
// )

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child, lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return l.child.Descriptor() // replace me
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	limit, _ := l.limitTups.EvalExpr(nil)
	curr := int64(0)
	return func() (*Tuple, error) {
		if curr == limit.(IntField).Value {
			return nil, nil
		}
		tuple, err := childIter()
		if err != nil {
			return nil, err
		}
		if tuple == nil {
			return nil, nil
		}
		curr += 1
		return tuple, nil
	}, nil // replace me
}
