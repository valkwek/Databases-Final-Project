package godb

type DeleteOp struct {
	// TODO: some code goes here
	deleteFile DBFile
	child      Operator
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	return &DeleteOp{deleteFile, child} // replace me
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return &TupleDesc{
		Fields: []FieldType{{Fname: "count", Ftype: IntType}},
	} // replace me
}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	insertions := int64(0)
	return func() (*Tuple, error) {
		for {
			tuple, err := childIter()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				break
			}
			err = dop.deleteFile.deleteTuple(tuple, tid)
			if err != nil {
				return nil, err
			}
			insertions++
		}
		return &Tuple{
			Desc: *dop.Descriptor(),
			Fields: []DBValue{
				IntField{insertions},
			},
		}, nil
	}, nil // replace me
}
