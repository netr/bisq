package bisq

import (
	"fmt"
	"strings"
)

type Builder struct {
	query         strings.Builder
	tableName     string
	whereBlocks   []Block
	limitBlock    Block
	offsetBlock   Block
	prevBlock     Block
	orderByBlocks []Block
}

type Block interface {
	String() string
}

type OrderByBlock struct {
	column    string
	direction string
}

func (o *OrderByBlock) String() string {
	return fmt.Sprintf("%v %v", o.column, o.direction)
}

type WhereBlock struct {
	column string
	value  interface{}
	op     string
}

type OffsetBlock struct {
	offset int
}

func (o *OffsetBlock) String() string {
	return fmt.Sprintf("OFFSET %v", o.offset)
}

type LimitBlock struct {
	limit int
}

func (l *LimitBlock) String() string {
	return fmt.Sprintf("LIMIT %v", l.limit)
}

func (w *WhereBlock) String() string {
	return fmt.Sprintf("%v %v %v", w.column, w.op, "[WHERE]")
}

type WhereNullBlock struct {
	column string
}

func (w *WhereNullBlock) String() string {
	return fmt.Sprintf("%v IS NULL", w.column)
}

type OrBlock struct{}

func (o *OrBlock) String() string {
	return "OR"
}

type WhereFnBlock struct {
	fn func(b *Builder)
}

func (w *WhereFnBlock) String() string {
	return ""

}

func Table(name string) *Builder {
	b := &Builder{
		tableName:     name,
		whereBlocks:   make([]Block, 0),
		orderByBlocks: []Block{},
	}
	return b
}

func (b *Builder) Get(columns ...string) *Builder {
	if len(columns) == 0 {
		columns = append(columns, "*")
	}

	b.query.WriteString("SELECT ")
	b.query.WriteString(strings.Join(columns, ", "))
	b.query.WriteString(" FROM ")
	b.query.WriteString(b.tableName)

	if len(b.whereBlocks) > 0 {
		b.query.WriteString(" WHERE ")
		whereQuery, _ := b.buildWhereClause(0)
		b.query.WriteString(whereQuery)
	}

	if len(b.orderByBlocks) > 0 {
		b.query.WriteString(" ORDER BY ")
		for idx, block := range b.orderByBlocks {
			if idx > 0 {
				b.query.WriteString(", ")
			}
			b.query.WriteString(block.String())
		}
	}

	if b.limitBlock != nil {
		b.query.WriteString(" ")
		b.query.WriteString(b.limitBlock.String())
	}

	if b.offsetBlock != nil {
		b.query.WriteString(" ")
		b.query.WriteString(b.offsetBlock.String())
	}
	b.query.WriteString(";")
	return b
}

func (b *Builder) Values() []interface{} {
	values := make([]interface{}, 0)
	return b.recursiveValues(values)
}

func (b *Builder) recursiveValues(values []interface{}) []interface{} {
	for _, block := range b.whereBlocks {
		if w, ok := block.(*WhereBlock); ok {
			values = append(values, w.value)
		}
		if wfn, ok := block.(*WhereFnBlock); ok {
			wBuilder := &Builder{
				tableName:     b.tableName,
				whereBlocks:   make([]Block, 0),
				orderByBlocks: []Block{},
			}
			wfn.fn(wBuilder)
			values = append(wBuilder.recursiveValues(values))
		}
	}
	return values
}

func (b *Builder) buildWhereClause(whereValue int) (string, int) {
	var subQuery strings.Builder

	for idx, block := range b.whereBlocks {
		var innerQuery strings.Builder
		switch v := block.(type) {
		case *WhereBlock:
			innerQuery.WriteString(strings.ReplaceAll(v.String(), "[WHERE]", fmt.Sprintf("%v", "$"+fmt.Sprintf("%v", whereValue+1))))
			whereValue++
		case *WhereNullBlock:
			innerQuery.WriteString(v.String())
		case *OrBlock:
			// Skip appending the "OR" block directly
		case *WhereFnBlock:
			nestedSubBuilder := &Builder{
				tableName:     b.tableName,
				whereBlocks:   make([]Block, 0),
				orderByBlocks: []Block{},
			}
			v.fn(nestedSubBuilder)
			nestedSubQuery, nestedWhereValue := nestedSubBuilder.buildWhereClause(whereValue)
			innerQuery.WriteString("(")
			innerQuery.WriteString(nestedSubQuery)
			innerQuery.WriteString(")")
			whereValue = nestedWhereValue
		}

		if idx > 0 {
			if _, ok := b.prevBlock.(*OrBlock); ok {
				subQuery.WriteString(" OR ")
			} else {
				if _, ok := block.(*OrBlock); !ok {
					subQuery.WriteString(" AND ")
				}
			}
		}

		subQuery.WriteString(innerQuery.String())

		b.prevBlock = block
	}

	return subQuery.String(), whereValue
}

func (b *Builder) Limit(limit int) *Builder {
	block := &LimitBlock{
		limit: limit,
	}
	b.limitBlock = block
	return b
}

func (b *Builder) Offset(offset int) *Builder {
	block := &OffsetBlock{
		offset: offset,
	}
	b.offsetBlock = block
	return b
}

func (b *Builder) OrderBy(column, direction string) *Builder {
	if strings.ToLower(direction) != "asc" && strings.ToLower(direction) != "desc" {
		direction = "asc"
	}
	direction = strings.ToUpper(direction)

	block := OrderByBlock{
		column:    column,
		direction: direction,
	}
	b.orderByBlocks = append(b.orderByBlocks, &block)
	return b
}

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

	b.whereBlocks = append(b.whereBlocks, block)
	return b
}

func (b *Builder) WhereNull(column string) *Builder {
	block := &WhereNullBlock{
		column: column,
	}
	b.whereBlocks = append(b.whereBlocks, block)
	return b
}

func (b *Builder) WhereFn(fn func(b *Builder)) *Builder {
	block := &WhereFnBlock{
		fn: fn,
	}
	b.whereBlocks = append(b.whereBlocks, block)
	return b
}

func (b *Builder) Or() *Builder {
	block := &OrBlock{}
	b.whereBlocks = append(b.whereBlocks, block)
	return b
}

func (b *Builder) String() string {
	return b.query.String()
}
