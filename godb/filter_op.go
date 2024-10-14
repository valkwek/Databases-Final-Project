package godb

// import (
// 	"fmt"
// 	"go/types"
// )

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

// Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.child.Descriptor() // replace me
}

// Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := f.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for {
			tuple, err := childIter()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				return nil, nil
			}
			leftFieldVal, err := f.left.EvalExpr(tuple)
			if err != nil {
				return nil, err
			}
			rightFieldVal, err := f.right.EvalExpr(tuple)
			if err != nil {
				return nil, err
			}
			if leftFieldVal.EvalPred(rightFieldVal, f.op) {
				return tuple, nil
			}
		}
	}, nil // replace me
}
