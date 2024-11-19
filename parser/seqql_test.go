package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ozontech/seq-db/conf"

	"github.com/ozontech/seq-db/seq"
)

func TestSeqQLAll(t *testing.T) {
	mapping := seq.Mapping{
		"service": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"level":   seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"message": seq.NewSingleType(seq.TokenizerTypeText, "", 0),
		"text":    seq.NewSingleType(seq.TokenizerTypeText, "", 0),
		"keyword": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		"уровень": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
	}
	test := func(in, out string) {
		t.Helper()
		seqql, err := ParseSeqQL(in, mapping)
		require.NoError(t, err)
		require.Equal(t, out, seqql.SeqQLString())
	}

	test("*", "*")

	// Propagate "not".
	test(`NOT NOT text:a`, `text:a`)
	test(`text:a AND NOT NOT text:b`, `(text:a and text:b)`)
	test(`text:a AND NOT text:b`, `(not text:b and text:a)`)
	test(`NOT NOT text:a OR text:b`, `(text:a or text:b)`)
	test(`NOT text:a OR text:b`, `(not (not text:b and text:a))`)
	test(`NOT (NOT text:a AND NOT text:b)`, `(text:a or text:b)`)
	test(`NOT text:a OR text:b OR text:c OR text:d`, `(not (not text:d and (not text:c and (not text:b and text:a))))`)
	test(`text:a OR text:b OR text:c OR NOT text:d`, `(not (not ((text:a or text:b) or text:c) and text:d))`)
	test(`NOT text:a AND text:b AND text:c AND text:d`, `(((not text:a and text:b) and text:c) and text:d)`)
	test(`text:a AND text:b AND text:c AND NOT text:d`, `(not text:d and ((text:a and text:b) and text:c))`)
	test(`NOT ((NOT text:a OR (NOT text:b AND text:c)) AND (NOT text:d AND NOT text:e))`, `((not (not text:b and text:c) and text:a) or (text:d or text:e))`)

	// Simple test cases.
	test(`service:some`, `service:some`)
	test(`service:"some text"`, `service:"some text"`)
	test(`text:"some text"`, `(text:some and text:text)`)
	test(`text:"some very long text"`, `(((text:some and text:very) and text:long) and text:text)`)
	test(`text:"a b" AND text:"c d f" or text:"e f"`, `(((text:a and text:b) and ((text:c and text:d) and text:f)) or (text:e and text:f))`)
	test(`service:"some*"`, `service:some*`)
	test(`service:some*`, `service:some*`)
	test(`service:"some*thing"`, `service:some*thing`)
	test(`service:some*thing`, `service:some*thing`)
	test(`service:"some*thing*"`, `service:some*thing*`)
	test(`service:some*thing*`, `service:some*thing*`)
	test(`service:*thing*`, `service:*thing*`)
	test(`service:*thing*`, `service:*thing*`)
	test(`service:"*"`, `service:*`)
	test(`service:*`, `service:*`)
	test(`service:some AND level:[1, 3] AND level:[3, 5]`, `((service:some and level:[1, 3]) and level:[3, 5])`)

	// Test case sensitivity.
	conf.CaseSensitive = true
	test("service: AbCdEf", "service:AbCdEf")
	test("text: AbCdEf", "text:AbCdEf")
	test("_exists_: 'AbCdEf'", "_exists_:AbCdEf")
	conf.CaseSensitive = false
	test("service: AbCdEf", "service:abcdef")
	test("text: AbCdEf", "text:abcdef")
	test("_exists_: `AbCdEf`", "_exists_:AbCdEf")

	// Test tokenization.
	test(`service:abc`, `service:abc`)
	test(`service:"quoted"`, `service:quoted`)
	test(`service:"quoted spaces"`, `service:"quoted spaces"`)
	test(`service:'"symbols"'`, `service:"\"symbols\""`)
	test(`service:"[1 TO 3]"`, `service:"[1 to 3]"`)
	test(`  service  :   hi  `, `service:hi`)
	test(`service:""`, `service:""`)
	test(`service:"cms*"`, `service:cms*`)
	test(`service:cms*`, `service:cms*`)
	test(`service:"cms*api"`, `service:cms*api`)
	test(`service:cms*api`, `service:cms*api`)
	test(`service:cms*inter*api`, `service:cms*inter*api`)
	test(`service:cms*inter*api`, `service:cms*inter*api`)
	test(`service:"cms"*"inter"*"api"`, `service:cms*inter*api`)
	test(`level:[1, 3]`, `level:[1, 3]`)
	test(`level:[*, 3]`, `level:[*, 3]`)
	test(`level:["*", 3]`, `level:[*, 3]`)
	test(`level:(1, "*"]`, `level:(1, *]`)
	test(`level:(1, *]`, `level:(1, *]`)
	test(`level:[1, 3] AND service:["*", "*"]`, `(level:[1, 3] and service:[*, *])`)
	test(`level:["from", "to"]`, `level:[from, to]`)
	test(`level:[from, to]`, `level:[from, to]`)
	test(`level:["a b c", "d e f"]`, `level:["a b c", "d e f"]`)
	test(`level:["hi", "ho"]`, `level:[hi, ho]`)
	test(`level:["-123", "-456"]`, `level:["-123", "-456"]`)
	test(`  level  :  [  1  ,  3  ]  `, `level:[1, 3]`)
	test(`level:["", "a\\*b"]`, `level:["", "a\\*b"]`)
	test(`level:["-3", 6) OR (service:"hel lo" AND level:[1, 3])`, `(level:["-3", 6) or (service:"hel lo" and level:[1, 3]))`)

	// Parsing AST.
	test(`service:"wms-svc-logistics-megasort" and level:""#`, `(service:"wms-svc-logistics-megasort" and level:"")`)
	test(`service: composer-api`, `service:"composer-api"`)
	test(`  service    : a   or   level     :   3  `, `(service:a or level:3)`)
	test(`service: a or level: 3 AND text:b`, `(service:a or (level:3 and text:b))`)
	test(`service: a or level: 3 or text:b`, `((service:a or level:3) or text:b)`)
	test(` not  service : a `, `(not service:a)`)
	test(`service:a or not service:b or service:c`, `(not (not service:c and (not service:a and service:b)))`)
	test(`not (service:a or service:c)`, `(not (service:a or service:c))`)
	test(`NOT Not service:a`, `service:a`)
	test(`service:*`, `service:*`)
	test(` service : * `, `service:*`)
	test(`service:a or service:b AND NOT service:c`, `(service:a or (not service:c and service:b))`)

	test(`#
# search by logistics-megasort service
service:"wms-svc-logistics-megasort" and level:"#"
# end of query`, `(service:"wms-svc-logistics-megasort" and level:"#")`)

	// Test text wildcards.
	test(`text:some*thing`, `text:some*thing`)
	test(`text:"a**b**"`, `text:a**b**`)
	test(`text:"some* weird* *cases"`, `((text:some* and text:weird*) and text:*cases)`)
	test(`text:"some *weird cases* hmm very*intrs"`, `((((text:some and text:*weird) and text:cases*) and text:hmm) and text:very*intrs)`)

	test(`text:"\\*\\**"`, `text:"\\*\\*"*`)
	test(`text:value=* AND text:value="\\*"*`, `((text:value and text:*) and (text:value and text:"\\*"*))`)
	test(`text:value="\\*\\*"* AND text:"\\*\\*"`, `((text:value and text:"\\*\\*"*) and text:"\\*\\*")`)
	// Complex search based on previous cases.
	test(`text:value=* AND text:value="\\*"* AND text:value="\\*\\*"* AND text:"\\*\\*" AND text:"\\*\\**"`,
		`(((((text:value and text:*) and (text:value and text:"\\*"*)) and (text:value and text:"\\*\\*"*)) and text:"\\*\\*") and text:"\\*\\*"*)`)

	test(`text:"val*" AND text:"val\\**"`, `(text:val* and text:"val\\*"*)`)
	// Test keyword wildcards.
	test(`service:**`, `service:**`)
	test(`service:a**`, `service:a**`)
	test(`service:**b`, `service:**b`)
	test(`service:a**b`, `service:a**b`)
	// Test quotes.
	test("keyword:`+7 995 28 07`", "keyword:\"+7 995 28 07\"")
	test("keyword:'+7 995 28 07'", "keyword:\"+7 995 28 07\"")
	test("keyword:`+7 995 ** **`", `keyword:"+7 995 \\*\\* \\*\\*"`)
	test("keyword:`+7 995 \\** **`", `keyword:"+7 995 \\\\*\\* \\*\\*"`)
	test("keyword:`\\t`", `keyword:"\\t"`)
	test(`keyword:"\t"`, `keyword:"\t"`)
	test(`keyword:"\\t"`, `keyword:"\\t"`)
	test(`keyword:"'\n\t'"`, `keyword:"'\n\t'"`)
	test(`message:"\"quoted string\""`,
		`(message:quoted and message:string)`)
	// Test UTF8.
	test(`text:"Произошла ошибка"`, `(text:произошла and text:ошибка)`)
	test("text:`Произошла ошибка: недостаточно места на диске`", `(((((text:произошла and text:ошибка) and text:недостаточно) and text:места) and text:на) and text:диске)`)
	test("уровень:😖", `уровень:"😖"`)
	// Test range.
	test(`level:[1, 3]`, `level:[1, 3]`)
	test(`level:(1, 3)`, `level:(1, 3)`)
	test(`level:["*", "*"]`, `level:[*, *]`)
	test(`level:[*, *]`, `level:[*, *]`)
	test(`level:[abc, cbd]`, `level:[abc, cbd]`)

	// Test separators without quotes.
	test(`service:clickhouse-shard-1`, `service:"clickhouse-shard-1"`)
	test(`x-forwarded-for: abc`, `"x-forwarded-for": abc`)
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
	test(`some field:abc`, `field "some" is not indexed`)
	test(`level service:abc`, `missing ':' after "level"`)
	test(`(level:3 AND level level:abc)`, `missing ':' after "level"`)
	test(`:"abc"`, `expected field name, got ":"`)
	test(`NOT (:"abc")`, `expected field name, got ":"`)
	test(`message:--||`, `unknown pipe: |`)
	test(`level:[** TO 1]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1 TO a*]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO a*b]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1 TO *b]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:["**" TO 1]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1 TO "a*"]`, `parsing range for field "level": expected ',' keyword, got "TO"`)
	test(`level:[1, "a*b"]`, `parsing range for field "level": only single wildcard is allowed`)
	test(`level:[1, "*b"]`, `parsing range for field "level": only single wildcard is allowed`)
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
	t.Parallel()
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

func TestSeqQLParsingASTStress(t *testing.T) {
	t.Parallel()
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
