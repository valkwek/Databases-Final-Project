package godb

import "fmt"

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	// You may want to add additional fields here
	// TODO: some code goes here
	distinct          bool
	encounteredTuples map[any]bool
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	if len(outputNames) != len(selectFields) {
		return nil, fmt.Errorf("outputNames and selectFields unequal length")
	}
	return &Project{
		selectFields:      selectFields,
		outputNames:       outputNames,
		child:             child,
		distinct:          distinct,
		encounteredTuples: make(map[any]bool),
	}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	outputDesc := TupleDesc{
		Fields: make([]FieldType, 0),
	}
	for i, field := range p.selectFields {
		outputDesc.Fields = append(outputDesc.Fields, FieldType{
			Fname: p.outputNames[i],
			Ftype: field.GetExprType().Ftype,
		})
	}
	return &outputDesc

}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIterator, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		for {
			tuple, err := childIterator()
			if err != nil {
				return nil, err
			}

			if tuple == nil {
				return nil, nil
			}

			projectedFields := make([]DBValue, 0)

			for _, fieldExpr := range p.selectFields {
				projectedFieldVal, err := fieldExpr.EvalExpr(tuple)
				if err != nil {
					return nil, err
				}
				projectedFields = append(projectedFields, projectedFieldVal)
			}

			outputTuple := Tuple{Desc: *p.Descriptor(), Fields: projectedFields, Rid: tuple.Rid}
			if p.distinct {
				if p.encounteredTuples[outputTuple.tupleKey()] {
					continue
				} else {
					p.encounteredTuples[outputTuple.tupleKey()] = true
					return &outputTuple, nil
				}
			} else {
				return &outputTuple, nil
			}
		}
	}, nil
}
