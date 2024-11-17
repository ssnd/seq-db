package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/seq"
)

func TestSeqQLParsingAST(t *testing.T) {
	tests := []astTest{
		{
			name:  `empty`,
			query: `*`,
			exp:   `*`,
		},
		//{
		//	name:  `in`,
		//	query: `service:in("hello", world, "wild"*"card","", "c", *"prefix", "in"*"fix", postfix*)`,
		//	exp:   ``,
		//},
		{
			name: "comments",
			query: `#
# search by logistics-megasort service
service:"wms-svc-logistics-megasort" and level:"#"
# end of query`,
			exp: `(service:"wms-svc-logistics-megasort" and level:"#")`,
		},
		{
			name:  "ends with comment",
			query: `service:"wms-svc-logistics-megasort" and level:""#`,
			exp:   `(service:"wms-svc-logistics-megasort" and level:"")`,
		},
		{
			name:  `simple_0`,
			query: `service: composer-api`,
			exp:   `service:composer-api`,
		},
		{
			name:  `simple_1`,
			query: `  s    : a   or   l     :   3  `,
			exp:   `(s:a or l:3)`,
		},
		{
			name:  `simple_2`,
			query: `s: a or l: 3 AND q:b`,
			exp:   `(s:a or (l:3 and q:b))`,
		},
		{
			name:  `simple_3`,
			query: `s: a or l: 3 or q:b`,
			exp:   `((s:a or l:3) or q:b)`,
		},
		{
			name:  `simple_4`,
			query: ` not  s : a `,
			exp:   `(not s:a)`,
		},
		{
			name:  `simple_5`,
			query: `s:a or not s:b or s:c`,
			exp:   `(not (not s:c and (not s:a and s:b)))`,
		},
		{
			name:  `simple_6`,
			query: `not (s:a or s:c)`,
			exp:   `(not (s:a or s:c))`,
		},
		{
			name:  `simple_7`,
			query: `NOT Not s:a`,
			exp:   `s:a`,
		},
		{
			name:  `wildcard_0`,
			query: `service:*`,
			exp:   `service:*`,
		},
		{
			name:  `wildcard_1`,
			query: ` service : * `,
			exp:   `service:*`,
		},
		{
			name:  `wildcard_2`,
			query: `a:a or b:b AND NOT c:c`,
			exp:   `(a:a or (not c:c and b:b))`,
		},
	}
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			act, err := ParseSeqQL(tst.query, nil)
			require.NoError(t, err)

			genStr := act.String()
			assert.Equal(t, tst.exp, genStr)
			second, err := ParseSeqQL(genStr, nil)
			require.NoError(t, err)
			assert.Equal(t, genStr, second.String())
		})
	}
}

func TestSeqQLParsingASTStress(t *testing.T) {
	iterations := 5000
	if testing.Short() {
		iterations = 50
	}
	for i := 0; i < iterations; i++ {
		exp := &ASTNode{}
		for i := 0; i < 100; i++ {
			addOperator(exp, 2*i)
			checkSelf(t, exp)
		}
	}
}

// error messages are actually readable
func TestParseSeqQLError(t *testing.T) {
	test := func(got, expected string) {
		t.Helper()
		q, err := ParseSeqQL(got, seq.TestMapping)
		require.NotNilf(t, err, "%+v", q)
		require.Equalf(t, expected, err.Error(), "%s != %s", expected, err.Error())
	}

	test(`service:`, `missing filter value for field "service"`)
	test(`service:"some`, `expected filter value for field "service", got "`)
	test(`service:some "`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:some"`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:some"service:clickhouse`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some"*"thing`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some"*thing"`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some"*"thing`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:"some" *"thing"`, `expected 'and', 'or', 'not', got: "*"`)
	test(`service: some thing`, `expected 'and', 'or', 'not', got: "thing"`)
	test(`service:"some thing`, `expected filter value for field "service", got "`)
	test(`service:    some"thing`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:`, `missing filter value for field "service"`)
	test(`service: some thing"`, `expected 'and', 'or', 'not', got: "thing"`)
	test(`AND`, `expected field name, got "AND"`)
	test(`NOT`, `unexpected end of query`)
	test(`service: AND level: 3`, `expected filter value for field "service", got AND`)
	test(`service: some AND level:`, `missing filter value for field "level"`)
	test(`nosuchfieldinlist: some`, `field "nosuchfieldinlist" is not indexed`)
	test(`service:"some text AND level:"3"`, `expected 'and', 'or', 'not', got: "\""`)
	test(`service:some text" AND level:"3"`, `expected 'and', 'or', 'not', got: "text"`)
	test(`m:a AND OR m:b`, `expected field name, got "OR"`)
	test(`m:a NOT AND m:b`, `expected 'and', 'or', 'not', got: "NOT"`)
	test(`m:a NOT`, `expected 'and', 'or', 'not', got: "NOT"`)
	test(`NOT NOT`, `unexpected end of query`)
	test(`level:[1 3]`, `parsing range for field "level": expected ',' keyword, got "3"`)
	test(`level:[1TO3]`, `parsing range for field "level": expected ',' keyword, got "]"`)
	test(`level:[1 TO 3`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:1 TO 3]`, `expected 'and', 'or', 'not', got: "TO"`)
	test(`level:[]`, `parsing range for field "level": unexpected token "]"`)
	test(`level:[1 TO [3]]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO 3]]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[[1 TO 3]]`, `parsing range for field "level": unexpected token "["`)
	test(`level:[[1 TO 3]`, `parsing range for field "level": unexpected token "["`)
	test(`level:[1 TP 3]`, `parsing range for field "level": expected ',' keyword, got "TP"`)
	test(`level:[1 TO 3[`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:]1 TO 3]`, `expected filter value for field "level", got ]`)
	test(`:some`, `expected field name, got ":"`)
	test(`:[1 TO 3]`, `expected field name, got ":"`)
	test(`[1 TO 3]:some`, `expected field name, got "["`)
	test(`(m:a`, `missing ')'`)
	test(`m:a)`, `expected 'and', 'or', 'not', got: ")"`)
	test(`m:a AND (`, `unexpected end of query`)
	test(`m:a (`, `expected 'and', 'or', 'not', got: "("`)
	test(`m:a )`, `expected 'and', 'or', 'not', got: ")"`)
	test(`m:a( AND m:a`, `expected 'and', 'or', 'not', got: "("`)
	test(`m:a (AND m:a)`, `expected 'and', 'or', 'not', got: "("`)
	test(`m:a) AND m:a`, `expected 'and', 'or', 'not', got: ")"`)
	test(`service:**`, `parsing keyword for field "service": duplicate wildcard`)
	test(`service:a**`, `parsing keyword for field "service": duplicate wildcard`)
	test(`service:**b`, `parsing keyword for field "service": duplicate wildcard`)
	test(`service:a**b`, `parsing keyword for field "service": duplicate wildcard`)
	test(`some field:abc`, `field "some" is not indexed`)
	test(`level service:abc`, `missing ':' after "level"`)
	test(`(level:3 AND level level:abc)`, `missing ':' after "level"`)
	test(`:"abc"`, `expected field name, got ":"`)
	test(`NOT (:"abc")`, `expected field name, got ":"`)
	test(`message:--||`, `unknown pipe: |`)
	test(`level:[** TO 1]`, `parsing range for field "level": expected ',' keyword, got "*"`)
	test(`level:[1 TO a*]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO a*b]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO *b]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:["**" TO 1]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO "a*"]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO "a*b"]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO "*b"]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:[ `, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:[1`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:[ 1`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:[*`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:[ *`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:["1"`, `parsing range for field "level": expected ',' keyword, got ""`)
	test(`level:["1`, `parsing range for field "level": unexpected token "\""`)
	test(`level:[ 1 to`, `parsing range for field "level": expected ',' keyword, got "to"`)
	test(`level:[1 to`, `parsing range for field "level": expected ',' keyword, got "to"`)
	test(`level:[1 to *`, `parsing range for field "level": expected ',' keyword, got "to"`)
	test(`level:[1 to 2`, `parsing range for field "level": expected ',' keyword, got "to"`)
	test(`level:[1 to 2*`, `parsing range for field "level": expected ',' keyword, got "to"`)
	test(`level:[1 to "2`, `parsing range for field "level": expected ',' keyword, got "to"`)
	test(`level:[1 to "2"`, `parsing range for field "level": expected ',' keyword, got "to"`)
	test(`level:[1]`, `parsing range for field "level": expected ',' keyword, got "]"`)
	test(`level:[*]`, `parsing range for field "level": expected ',' keyword, got "]"`)
	test(`level:[1 to "2]`, `parsing range for field "level": expected ',' keyword, got "to"`)
}

func TestSeqQLParserFuzz(t *testing.T) {
	// test, that any permutation of these characters will be invalid
	// template must be <= 11 symbols, or test will be very long
	templates := []string{
		`m:a[]`,
		`m::a`,
		`m:::a`,
		`m:a("`,
		`m:()`,
		`m:"`,
		`m:()\`,
		`:()""\`,
		`m:a OR ()"`,
		`AND OR NOT`,
	}
	for _, template := range templates {
		t.Run("test", func(t *testing.T) {
			if len(template) >= 12 {
				panic("template is too long")
			}
			if len(template) >= 10 && testing.Short() {
				t.Skip("skipping long template test")
			}
			for p := make([]int, len(template)); p[0] < len(p); nextPerm(p) {
				s := getPerm(p, template)

				_, err := ParseSeqQL(s, nil)
				require.Errorf(t, err, "query: %s", s)
			}
		})
	}
}

func TestSeqQLAll(t *testing.T) {
	tests := []testCase{
		{
			name:   `simple_0`,
			query:  `service:some`,
			expect: `service:some`,
		},
		{
			name:   `simple_1`,
			query:  `service:"some text"`,
			expect: `service:"some text"`,
		},
		{
			name:   `simple_2`,
			query:  `text:"some text"`,
			expect: `(text:some and text:text)`,
		},
		{
			name:   `simple_3`,
			query:  `text:"some very long text"`,
			expect: `(((text:some and text:very) and text:long) and text:text)`,
		},
		{
			name:   `simple_4`,
			query:  `text:"a b" AND text:"c d f" or text:"e f"`,
			expect: `(((text:a and text:b) and ((text:c and text:d) and text:f)) or (text:e and text:f))`,
		},
		{
			name:   `wildcard_0`,
			query:  `service:"some*"`,
			expect: `service:"some*"`,
		},
		{
			name:   `wildcard_0`,
			query:  `service:"some"*`,
			expect: `service:some*`,
		},
		{
			name:   `wildcard_1`,
			query:  `service:"some*thing"`,
			expect: `service:"some*thing"`,
		},
		{
			name:   `wildcard_1`,
			query:  `service:some*thing`,
			expect: `service:some*thing`,
		},
		{
			name:   `wildcard_2`,
			query:  `service:"some*thing*"`,
			expect: `service:"some*thing*"`,
		},
		{
			name:   `wildcard_2`,
			query:  `service:some*thing*`,
			expect: `service:some*thing*`,
		},
		{
			name:   `wildcard_3`,
			query:  `service:"*thing*"`,
			expect: `service:"*thing*"`,
		},
		{
			name:   `wildcard_3`,
			query:  `service:*thing*`,
			expect: `service:*thing*`,
		},
		{
			name:   `wildcard_4`,
			query:  `service:"*"`,
			expect: `service:"*"`,
		},
		{
			name:   `wildcard_4`,
			query:  `service:*`,
			expect: `service:*`,
		},
		{
			name:   `wildcard_5`,
			query:  `text:some*thing`,
			expect: `text:some*thing`,
		},
		{
			name:   `wildcard_6`,
			query:  `text:"a**b**"`,
			expect: `(text:a and text:b)`,
		},
		{
			name:   `range_0`,
			query:  `level:[1, 3]`,
			expect: `level:[1, 3]`,
		},
		{
			name:   `range_1`,
			query:  `level:(1, 3)`,
			expect: `level:(1, 3)`,
		},
		{
			name:   `range_2`,
			query:  `level:["*", "*"]`,
			expect: `level:["*", "*"]`,
		},
		{
			name:   `range_2`,
			query:  `level:[*, *]`,
			expect: `level:[*, *]`,
		},
		{
			name:   `range_3`,
			query:  `level:[abc, cbd]`,
			expect: `level:[abc, cbd]`,
		},
		{
			name:   `range_4`,
			query:  `service:some AND level:[1, 3] AND level:[3, 5]`,
			expect: `((service:some and level:[1, 3]) and level:[3, 5])`,
		},
	}
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			expr, err := ParseSeqQL(tst.query, seq.TestMapping)
			require.NoError(t, err)
			assert.Equalf(t, tst.expect, expr.Root.SeqQLString(), "%+v", expr.Root)
		})
	}
}

func TestSeqQLTokenization(t *testing.T) {
	tests := []testCase{
		{
			name:   `token_0`,
			query:  `service:abc`,
			expect: `service:abc`,
		},
		{
			name:   `token_1`,
			query:  `service:"quoted"`,
			expect: `service:quoted`,
		},
		{
			name:   `token_2`,
			query:  `service:"quoted spaces"`,
			expect: `service:"quoted spaces"`,
		},
		{
			name:   `token_3`,
			query:  `service:'"symbols"'`,
			expect: `service:"\"symbols\""`,
		},
		{
			name:   `token_4`,
			query:  `message:"[1 TO 3]"`,
			expect: `message:"[1 to 3]"`,
		},
		{
			name:   `token_5`,
			query:  `  message  :   hi  `,
			expect: `message:hi`,
		},
		{
			name:   `token_6`,
			query:  `MiXeD_CaSe:TeSt`,
			expect: `MiXeD_CaSe:test`,
		},
		{
			name:   `token_7`,
			query:  `MiXeD_CaSe:"TeSt"`,
			expect: `MiXeD_CaSe:test`,
		},
		{
			name:   `token_8`,
			query:  `service:""`,
			expect: `service:""`,
		},
		{
			name:   `wildcard_0`,
			query:  `service:"cms*"`,
			expect: `service:"cms*"`,
		},
		{
			name:   `wildcard_0`,
			query:  `service:cms*`,
			expect: `service:cms*`,
		},
		{
			name:   `wildcard_1`,
			query:  `service:"cms*api"`,
			expect: `service:"cms*api"`,
		},
		{
			name:   `wildcard_1`,
			query:  `service:cms*api`,
			expect: `service:cms*api`,
		},
		{
			name:   `wildcard_2`,
			query:  `service:cms*inter*api`,
			expect: `service:cms*inter*api`,
		},
		{
			name:   `wildcard_3`,
			query:  `service:cms*inter*api`,
			expect: `service:cms*inter*api`,
		},
		{
			name:   `wildcard_4`,
			query:  `service:"cms"*"inter"*"api"`,
			expect: `service:cms*inter*api`,
		},
		{
			name:   `range_0`,
			query:  `level:[1, 3]`,
			expect: `level:[1, 3]`,
		},
		{
			name:   `range_1`,
			query:  `level:[*, 3]`,
			expect: `level:[*, 3]`,
		},
		{
			name:   `range_1`,
			query:  `level:["*", 3]`,
			expect: `level:["*", 3]`,
		},
		{
			name:   `range_2`,
			query:  `level:(1, "*"]`,
			expect: `level:(1, "*"]`,
		},
		{
			name:   `range_2`,
			query:  `level:(1, *]`,
			expect: `level:(1, *]`,
		},
		{
			name:   `range_3`,
			query:  `level:[1, 3] AND id:["*", "*"]`,
			expect: `(level:[1, 3] and id:["*", "*"])`,
		},
		{
			name:   `range_4`,
			query:  `level:["from", "to"]`,
			expect: `level:[from, to]`,
		},
		{
			name:   `range_5`,
			query:  `level:[from, to]`,
			expect: `level:[from, to]`,
		},
		{
			name:   `range_6`,
			query:  `level:["a b c", "d e f"]`,
			expect: `level:["a b c", "d e f"]`,
		},
		{
			name:   `range_7`,
			query:  `level:["hi", "ho"]`,
			expect: `level:[hi, ho]`,
		},
		{
			name:   `range_8`,
			query:  `level:["-123", "-456"]`,
			expect: `level:["-123", "-456"]`,
		},
		{
			name:   `range_9`,
			query:  `  level  :  [  1  ,  3  ]  `,
			expect: `level:[1, 3]`,
		},
		{
			name:   `range_10`,
			query:  `level:["", "a\\*b"]`,
			expect: `level:["", "a\\*b"]`,
		},
		{
			name:   `complex_0`,
			query:  `id:["-3", 6) OR (message:"hel lo" AND level:[1, 3])`,
			expect: `(id:["-3", 6) or (message:"hel lo" and level:[1, 3]))`,
		},
	}
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			expr, err := ParseSeqQL(tst.query, nil)
			require.NoError(t, err)
			assert.Equal(t, tst.expect, expr.Root.SeqQLString())
		})
	}
}

func TestSeqQLTokenizationCaseSensitive(t *testing.T) {
	tests := []testCase{
		{
			name:   `case_0`,
			query:  `service:AbCdEf`,
			expect: `service:AbCdEf`,
		},
		{
			name:   `case_0`,
			query:  `service:"AbC"`,
			expect: `service:AbC`,
		},
	}
	conf.CaseSensitive = true
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			expr, err := ParseSeqQL(tst.query, nil)
			require.NoError(t, err)
			assert.Equal(t, tst.expect, expr.Root.String())
		})
	}
}

func TestSeqQLExistsCaseSensitive(t *testing.T) {
	q := `_exists_:AbCdEfG`
	for _, cs := range []bool{true, false} {
		conf.CaseSensitive = cs
		expr, err := ParseSeqQL(q, nil)
		assert.NoError(t, err)
		assert.Equal(t, expr.Root.String(), q)
	}
}

func TestSeqQLPropagateNot(t *testing.T) {
	tests := []testCase{
		{
			name:   `double_not`,
			query:  `NOT NOT m:a`,
			expect: `m:a`,
		},
		{
			name:   `and_double_not`,
			query:  `m:a AND NOT NOT m:b`,
			expect: `(m:a AND m:b)`,
		},
		{
			name:   `nand`,
			query:  `m:a AND NOT m:b`,
			expect: `(NOT m:b AND m:a)`,
		},
		{
			name:   `or_double_not`,
			query:  `NOT NOT m:a OR m:b`,
			expect: `(m:a OR m:b)`,
		},
		{
			name:   `nor`,
			query:  `NOT m:a OR m:b`,
			expect: `(NOT (NOT m:b AND m:a))`,
		},
		{
			name:   `propagate_and`,
			query:  `NOT (NOT m:a AND NOT m:b)`,
			expect: `(m:a OR m:b)`,
		},
		{
			name:   `or_tree_left`,
			query:  `NOT m:a OR m:b OR m:c OR m:d`,
			expect: `(NOT (NOT m:d AND (NOT m:c AND (NOT m:b AND m:a))))`,
		},
		{
			name:   `or_tree_right`,
			query:  `m:a OR m:b OR m:c OR NOT m:d`,
			expect: `(NOT (NOT ((m:a OR m:b) OR m:c) AND m:d))`,
		},
		{
			name:   `and_tree_left`,
			query:  `NOT m:a AND m:b AND m:c AND m:d`,
			expect: `(((NOT m:a AND m:b) AND m:c) AND m:d)`,
		},
		{
			name:   `and_tree_right`,
			query:  `m:a AND m:b AND m:c AND NOT m:d`,
			expect: `(NOT m:d AND ((m:a AND m:b) AND m:c))`,
		},
		{
			name:   `big_tree`,
			query:  `NOT ((NOT m:a OR (NOT m:b AND m:c)) AND (NOT m:d AND NOT m:e))`,
			expect: `((NOT (NOT m:b AND m:c) AND m:a) OR (m:d OR m:e))`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node, err := ParseSeqQL(test.query, nil)
			require.NoError(t, err)
			assert.Equal(t, test.expect, node.Root.String())
		})
	}
}

//func TestSeqQLWildcardText(t *testing.T) {
//	tests := []testCase{
//		{
//			query:  `text:"some* weird* *cases"`,
//			expect: `((text:some* AND text:weird*) AND text:*cases)`,
//		},
//		{
//			query:  `text:"some *weird cases* hmm very*intrs"`,
//			expect: `((((text:some AND text:*weird) AND text:cases*) AND text:hmm) AND text:very*intrs)`,
//		},
//		{
//			query:  `text:"value=*" AND text:"value=\**" AND text:"value=\*\**" AND text:"\*\*" AND text:"\*\**"`,
//			expect: `(((((text:value AND text:*) AND (text:value AND text:\**)) AND (text:value AND text:\*\**)) AND text:\*\*) AND text:\*\**)`,
//		},
//		{
//			query:  `text:"val*" AND text:"val\**"`,
//			expect: `(text:val* AND text:val\**)`,
//		},
//	}
//
//	for _, test := range tests {
//		t.Run("wildcard", func(t *testing.T) {
//			ast, err := ParseSeqQL(test.query, seq.TestMapping)
//			require.NoErrorf(t, err, "%s", test.query)
//			assert.Equalf(t, test.expect, ast.Root.String(), "%s", test.query)
//		})
//	}
//}

func BenchmarkSeqQLParsing(b *testing.B) {
	var query SeqQLQuery
	var err error
	str := `service: "some service" AND level:1`
	for i := 0; i < b.N; i++ {
		query, err = ParseSeqQL(str, seq.TestMapping)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
	exp = query.Root
}

func BenchmarkSeqQLParsingLong(b *testing.B) {
	var query SeqQLQuery
	var err error
	str := `((NOT ((((m:19 OR m:20) OR m:18) AND m:16) OR ((NOT (m:25 OR m:26)) AND m:12))) OR (((NOT m:29) AND m:22) OR (((m:31 OR m:32) AND m:14) OR (m:27 AND m:28))))`
	for i := 0; i < b.N; i++ {
		query, err = ParseSeqQL(str, seq.TestMapping)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
	exp = query.Root
}
