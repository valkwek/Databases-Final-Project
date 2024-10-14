package godb

import "fmt"

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	// You may want to add additional fields here
	// TODO: some code goes here
	distinct bool
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	if len(selectFields) != len(outputNames) {
		return nil, fmt.Errorf("selectFields is a list of expressions that represents the fields to be selected, outputNames are names by which the selected fields are named (should be same length as selectFields; throws error if not)")
	}
	return &Project{selectFields, outputNames, child, distinct}, nil // replace me
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fields := make([]FieldType, len(p.selectFields))
	for i, expr := range p.selectFields {
		fields[i] = FieldType{Fname: p.outputNames[i], Ftype: expr.GetExprType().Ftype}
	}
	return &TupleDesc{Fields: fields} // replace me
}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	distinctTuples := make(map[string]bool)
	return func() (*Tuple, error) {
		for {
			tuple, err := childIter()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				return nil, nil
			}
			projectedFields := make([]DBValue, len(p.outputNames))
			for i, expr := range p.selectFields {
				projectedFieldValue, err := expr.EvalExpr(tuple)
				if err != nil {
					return nil, err
				}
				projectedFields[i] = projectedFieldValue
			}
			projectedTuple := &Tuple{Desc: *p.Descriptor(), Fields: projectedFields, Rid: tuple.Rid}
			if _, notDistinct := distinctTuples[projectedTuple.PrettyPrintString(false)]; !notDistinct {
				distinctTuples[projectedTuple.PrettyPrintString(false)] = true
				return projectedTuple, nil // replace me
			}
		}
	}, nil
}
