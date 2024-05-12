package bisq_test

import (
	"github.com/netr/bisq"
	"github.com/stretchr/testify/suite"
	"testing"
)

type BisqSuite struct {
	suite.Suite
}

func (s *BisqSuite) Test_Select() {
	tests := []struct {
		name             string
		builder          *bisq.Builder
		expected         string
		expectedValueLen int
	}{
		{
			name:     "query: select all from table",
			builder:  bisq.Table("carriers").Get(),
			expected: "SELECT * FROM carriers;",
		},
		{
			name:     "query: select specific columns from table",
			builder:  bisq.Table("carriers").Get("id", "name"),
			expected: "SELECT id, name FROM carriers;",
		},
		{
			name:             "query: select all from table where",
			builder:          bisq.Table("conversations").Where("user_id", 1).Get(),
			expected:         "SELECT * FROM conversations WHERE user_id = $1;",
			expectedValueLen: 1,
		},
		{
			name:             "query: select all from table where multiple conditions",
			builder:          bisq.Table("conversations").Where("user_id", 1).Where("status", "active").Get(),
			expected:         "SELECT * FROM conversations WHERE user_id = $1 AND status = $2;",
			expectedValueLen: 2,
		},
		{
			name:     "query: select order by column",
			builder:  bisq.Table("campaign_messages").OrderBy("id", "DESC").Get(),
			expected: "SELECT * FROM campaign_messages ORDER BY id DESC;",
		},
		{
			name:     "query: select order by column",
			builder:  bisq.Table("campaign_messages").Limit(1).Get(),
			expected: "SELECT * FROM campaign_messages LIMIT 1;",
		},
		{
			name:             "query: select order by column",
			builder:          bisq.Table("total_stats").Where("date", ">=", "2021-01-01").Where("date", "<=", "2021-01-31").Get(),
			expected:         "SELECT * FROM total_stats WHERE date >= $1 AND date <= $2;",
			expectedValueLen: 2,
		},
		{
			name:             "query: select order by column",
			builder:          bisq.Table("lead_enrichments").Where("import_file_id", 1).WhereNull("blacklisted_at").Get("lead_id"),
			expected:         "SELECT lead_id FROM lead_enrichments WHERE import_file_id = $1 AND blacklisted_at IS NULL;",
			expectedValueLen: 1,
		},
		{
			name: "query: select order by column",
			builder: bisq.Table("leads").WhereFn(func(b *bisq.Builder) {
				b.Where("import_file_id", 1).Or().WhereNull("blacklisted_at")
			}).Get(),
			expected:         "SELECT * FROM leads WHERE (import_file_id = $1 OR blacklisted_at IS NULL);",
			expectedValueLen: 1,
		},
		{
			name:     "query: select with offset and limit",
			builder:  bisq.Table("users").OrderBy("created_at", "DESC").Limit(10).Offset(20).Get("id", "name", "email"),
			expected: "SELECT id, name, email FROM users ORDER BY created_at DESC LIMIT 10 OFFSET 20;",
		},
		{
			name:             "query: select with complex where conditions",
			builder:          bisq.Table("products").Where("category", "electronics").Where("price", ">", 1000).Where("price", "<", 2000).OrderBy("price", "ASC").Get(),
			expected:         "SELECT * FROM products WHERE category = $1 AND price > $2 AND price < $3 ORDER BY price ASC;",
			expectedValueLen: 3,
		},
		{
			name: "query: select with nested where conditions",
			builder: bisq.Table("orders").WhereFn(func(b *bisq.Builder) {
				b.Where("status", "pending").Or().WhereFn(func(b *bisq.Builder) {
					b.Where("status", "processing").Where("created_at", ">", "2023-01-01")
				})
			}).Get("id", "user_id", "total"),
			expected:         "SELECT id, user_id, total FROM orders WHERE (status = $1 OR (status = $2 AND created_at > $3));",
			expectedValueLen: 3,
		},
		{
			name:     "query: select with multiple order by columns",
			builder:  bisq.Table("employees").OrderBy("department", "ASC").OrderBy("salary", "DESC").Limit(5).Get("id", "name", "department", "salary"),
			expected: "SELECT id, name, department, salary FROM employees ORDER BY department ASC, salary DESC LIMIT 5;",
		},
		{
			name: "query: select with complex where conditions and nested where conditions",
			builder: bisq.Table("articles").Where("published", true).WhereFn(func(b *bisq.Builder) {
				b.Where("category", "tech").Or().Where("category", "science")
			}).WhereFn(func(b *bisq.Builder) {
				b.Where("views", ">", 1000).Or().Where("featured", true)
			}).OrderBy("published_at", "DESC").Limit(10).Get("id", "title", "category", "views"),
			expected:         "SELECT id, title, category, views FROM articles WHERE published = $1 AND (category = $2 OR category = $3) AND (views > $4 OR featured = $5) ORDER BY published_at DESC LIMIT 10;",
			expectedValueLen: 5,
		},
	}

	for _, tt := range tests {
		s.Equal(tt.expected, tt.builder.String(), tt.name)
		s.Len(tt.builder.Values(), tt.expectedValueLen, tt.name)
	}
}

func TestBisqSuite(t *testing.T) {
	suite.Run(t, new(BisqSuite))
}
