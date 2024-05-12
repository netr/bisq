package bisq

import (
	"fmt"
	"strings"
)

type Block interface {
	String() string
}

// OrderByBlock is a block that represents an ORDER BY condition. Can be stacked to order by multiple columns. Defaults to ascending order.
type OrderByBlock struct {
	column    string
	direction string
}

func (o *OrderByBlock) String() string {
	return fmt.Sprintf("%v %v", o.column, o.direction)
}

// WhereBlock is a block that represents a WHERE condition
type WhereBlock struct {
	column string
	value  interface{}
	op     string
}

func (w *WhereBlock) String() string {
	return fmt.Sprintf("%v %v %v", w.column, w.op, "[WHERE]")
}

// OffsetBlock is a block that represents an OFFSET condition
type OffsetBlock struct {
	offset int
}

func (o *OffsetBlock) String() string {
	return fmt.Sprintf("OFFSET %v", o.offset)
}

// LimitBlock is a block that represents a LIMIT condition
type LimitBlock struct {
	limit int
}

func (l *LimitBlock) String() string {
	return fmt.Sprintf("LIMIT %v", l.limit)
}

// WhereNullBlock is a block that represents a WHERE column IS NULL condition
type WhereNullBlock struct {
	column string
}

func (w *WhereNullBlock) String() string {
	return fmt.Sprintf("%v IS NULL", w.column)
}

// OrBlock is a block that represents an OR condition
type OrBlock struct{}

func (o *OrBlock) String() string {
	return "OR"
}

type Builder struct {
	query     strings.Builder // Query string
	tableName string          // Table name
	wheres    []Block         // WhereBlock, WhereNullBlock, OrBlock, WhereFnBlock
	limit     Block           // LimitBlock
	offset    Block           // OffsetBlock
	prev      Block           // used to determine if the previous block was an OrBlock
	orderBys  []Block         // OrderByBlock
}

// WhereFnBlock is a block that allows for nested where conditions
type WhereFnBlock struct {
	fn func(b *Builder)
}

func (w *WhereFnBlock) String() string {
	return ""

}

// Table creates a new Builder instance with the table name
func Table(name string) *Builder {
	b := &Builder{
		tableName: name,
		wheres:    make([]Block, 0),
		orderBys:  []Block{},
	}
	return b
}

func (b *Builder) String() string {
	return b.query.String()
}

func (b *Builder) Values() []interface{} {
	values := make([]interface{}, 0)
	return b.recursiveValues(values)
}

func (b *Builder) recursiveValues(values []interface{}) []interface{} {
	for _, block := range b.wheres {
		if w, ok := block.(*WhereBlock); ok {
			values = append(values, w.value)
		}
		if wfn, ok := block.(*WhereFnBlock); ok {
			wBuilder := &Builder{
				tableName: b.tableName,
				wheres:    make([]Block, 0),
				orderBys:  []Block{},
			}
			wfn.fn(wBuilder)
			values = append(wBuilder.recursiveValues(values))
		}
	}
	return values
}

func (b *Builder) Get(columns ...string) *Builder {
	if len(columns) == 0 {
		columns = append(columns, "*")
	}

	b.query.WriteString("SELECT ")
	b.query.WriteString(strings.Join(columns, ", "))
	b.query.WriteString(" FROM ")
	b.query.WriteString(b.tableName)

	if len(b.wheres) > 0 {
		b.query.WriteString(" WHERE ")
		whereQuery, _ := b.buildWhereClause(0)
		b.query.WriteString(whereQuery)
	}

	if len(b.orderBys) > 0 {
		b.query.WriteString(" ORDER BY ")
		for idx, block := range b.orderBys {
			if idx > 0 {
				b.query.WriteString(", ")
			}
			b.query.WriteString(block.String())
		}
	}

	if b.limit != nil {
		b.query.WriteString(" ")
		b.query.WriteString(b.limit.String())
	}

	if b.offset != nil {
		b.query.WriteString(" ")
		b.query.WriteString(b.offset.String())
	}
	b.query.WriteString(";")
	return b
}

func (b *Builder) buildWhereClause(whereValue int) (string, int) {
	var subSB strings.Builder

	for idx, block := range b.wheres {
		var innerSB strings.Builder
		switch v := block.(type) {
		case *WhereBlock:
			innerSB.WriteString(strings.ReplaceAll(v.String(), "[WHERE]", fmt.Sprintf("%v", "$"+fmt.Sprintf("%v", whereValue+1))))
			whereValue++
		case *WhereNullBlock:
			innerSB.WriteString(v.String())
		case *OrBlock:
			// Skip appending the "OR" block directly
		case *WhereFnBlock:
			innerBuilder := &Builder{
				tableName: b.tableName,
				wheres:    make([]Block, 0),
				orderBys:  []Block{},
			}
			v.fn(innerBuilder)
			innerSubQuery, innerSubWhereValue := innerBuilder.buildWhereClause(whereValue)
			innerSB.WriteString("(")
			innerSB.WriteString(innerSubQuery)
			innerSB.WriteString(")")
			whereValue = innerSubWhereValue
		}

		if idx > 0 {
			if _, ok := b.prev.(*OrBlock); ok {
				subSB.WriteString(" OR ")
			} else {
				if _, ok := block.(*OrBlock); !ok {
					subSB.WriteString(" AND ")
				}
			}
		}

		subSB.WriteString(innerSB.String())
		b.prev = block
	}

	return subSB.String(), whereValue
}

func (b *Builder) Limit(limit int) *Builder {
	block := &LimitBlock{
		limit: limit,
	}
	b.limit = block
	return b
}

func (b *Builder) Offset(offset int) *Builder {
	block := &OffsetBlock{
		offset: offset,
	}
	b.offset = block
	return b
}

// OrderBy adds an ORDER BY condition to the query
func (b *Builder) OrderBy(column, direction string) *Builder {
	if strings.ToLower(direction) != "asc" && strings.ToLower(direction) != "desc" {
		direction = "asc"
	}
	direction = strings.ToUpper(direction)

	block := OrderByBlock{
		column:    column,
		direction: direction,
	}
	b.orderBys = append(b.orderBys, &block)
	return b
}

// Where adds a WHERE condition to the query
func (b *Builder) Where(column string, value ...interface{}) *Builder {
	if len(value) == 0 {
		return b
	}

	block := &WhereBlock{
		column: column,
		value:  value[0],
		op:     "=",
	}
	if len(value) > 1 {
		block = &WhereBlock{
			column: column,
			value:  value[1],
			op:     fmt.Sprintf("%v", value[0]),
		}
	}

	b.wheres = append(b.wheres, block)
	return b
}

// WhereNull adds a WHERE column IS NULL condition to the query.
func (b *Builder) WhereNull(column string) *Builder {
	block := &WhereNullBlock{
		column: column,
	}
	b.wheres = append(b.wheres, block)
	return b
}

// WhereFn adds a WHERE closure that can be used to nest conditions and wrap them in parentheses.
func (b *Builder) WhereFn(fn func(b *Builder)) *Builder {
	block := &WhereFnBlock{
		fn: fn,
	}
	b.wheres = append(b.wheres, block)
	return b
}

// Or adds an OR condition to the query
func (b *Builder) Or() *Builder {
	block := &OrBlock{}
	b.wheres = append(b.wheres, block)
	return b
}
