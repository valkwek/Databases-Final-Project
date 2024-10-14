package godb

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	sum   any
}

func (a *SumAggState) Copy() AggState {
	// TODO: some code goes here
	return &SumAggState{a.alias, a.expr, a.sum} // replace me
}

func intAggGetter(v DBValue) any {
	// TODO: some code goes here
	value, ok := v.(IntField)
	if ok {
		return value.Value
	}
	return nil // replace me
}

func stringAggGetter(v DBValue) any {
	// TODO: some code goes here
	value, ok := v.(StringField)
	if ok {
		return value.Value
	}
	return nil // replace me
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.sum = nil
	a.expr = expr
	a.alias = alias
	return nil // replace me
}

func (a *SumAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	dbVal, _ := a.expr.EvalExpr(t)
	intValue := intAggGetter(dbVal)
	if intValue != nil {
		if a.sum == nil {
			a.sum = int64(0)
		}
		a.sum = a.sum.(int64) + intValue.(int64)
	} else {
		if a.sum == nil {
			a.sum = ""
		}
		a.sum = a.sum.(string) + stringAggGetter(dbVal).(string)
	}
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td // replace me
}

func (a *SumAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	var f DBValue
	if intValue, ok := a.sum.(int64); ok {
		f = IntField{intValue}
	} else {
		f = StringField{a.sum.(string)}
	}
	return &Tuple{*td, []DBValue{f}, nil} // replace me
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	total int64
	size  int64
}

func (a *AvgAggState) Copy() AggState {
	// TODO: some code goes here
	return &AvgAggState{a.alias, a.expr, a.total, a.size} // replace me
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.total = 0
	a.size = 0
	a.expr = expr
	a.alias = alias
	return nil // replace me
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	dbVal, err := a.expr.EvalExpr(t)
	if err == nil {
		a.total += dbVal.(IntField).Value
		a.size += 1
	}
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td // replace me
}

func (a *AvgAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	f := IntField{a.total / a.size}
	return &Tuple{*td, []DBValue{f}, nil} // replace me
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	max   any
}

func (a *MaxAggState) Copy() AggState {
	// TODO: some code goes here
	return &MaxAggState{a.alias, a.expr, a.max} // replace me
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.max = nil
	a.expr = expr
	a.alias = alias
	return nil // replace me
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	dbVal, _ := a.expr.EvalExpr(t)
	intValue := intAggGetter(dbVal)
	if intValue != nil {
		if a.max == nil {
			a.max = intValue.(int64)
		}
		if a.max.(int64) < intValue.(int64) {
			a.max = intValue.(int64)
		}
	} else {
		strValue := stringAggGetter(dbVal).(string)
		if a.max == nil {
			a.max = strValue
		}
		if a.max.(string) < strValue {
			a.max = strValue
		}
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td // replace me
}

func (a *MaxAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	var f DBValue
	if intValue, ok := a.max.(int64); ok {
		f = IntField{intValue}
	} else {
		f = StringField{a.max.(string)}
	}
	return &Tuple{*td, []DBValue{f}, nil} // replace me
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	// TODO: some code goes here
	alias string
	expr  Expr
	min   any
}

func (a *MinAggState) Copy() AggState {
	// TODO: some code goes here
	return &MinAggState{a.alias, a.expr, a.min} // replace me
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	a.min = nil
	a.expr = expr
	a.alias = alias
	return nil // replace me
}

func (a *MinAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	dbVal, _ := a.expr.EvalExpr(t)
	intValue := intAggGetter(dbVal)
	if intValue != nil {
		if a.min == nil {
			a.min = intValue.(int64)
		}
		if a.min.(int64) > intValue.(int64) {
			a.min = intValue.(int64)
		}
	} else {
		strValue := stringAggGetter(dbVal).(string)
		if a.min == nil {
			a.min = strValue
		}
		if a.min.(string) > strValue {
			a.min = strValue
		}
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td // replace me
}

func (a *MinAggState) Finalize() *Tuple {
	// TODO: some code goes here
	td := a.GetTupleDesc()
	var f DBValue
	if intValue, ok := a.min.(int64); ok {
		f = IntField{intValue}
	} else {
		f = StringField{a.min.(string)}
	}
	return &Tuple{*td, []DBValue{f}, nil} // replace me
}
