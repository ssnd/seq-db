package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePipeFields(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("* | fields  message,error, level", "* | fields message, error, level")
	test("* | fields level", "* | fields level")
	test("* | fields level", "* | fields level")
	test(`* | fields "_id"`, `* | fields _id`)
	test(`* | fields "_\\message\\_"`, `* | fields "_\\message\\_"`)
	test(`* | fields "_\\message*"`, `* | fields "_\\message\*"`)
	test(`* | fields k8s_namespace`, `* | fields k8s_namespace`)
}

func TestParsePipeRemove(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("* | remove message,error, level", "* | remove message, error, level")
	test("* | remove level", "* | remove level")
	test(`* | remove "_id"`, `* | remove _id`)
	test(`* | remove "_\\message\\_"`, `* | remove "_\\message\\_"`)
	test(`* | remove "_\\message*"`, `* | remove "_\\message\*"`)
	test(`* | remove k8s_namespace`, `* | remove k8s_namespace`)
}

func TestParsePipeWhere(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("* | where __MESSAGE__:`error*`*", `* | where __MESSAGE__:"error\*"*`)
	test("* | where 'level':`info`", "* | where level:info")
	test("* | where level:error | where message:error | where _id:42", "* | where level:error | where message:error | where _id:42")
	test(`* | where "User-Agent":"curl"`, `* | where User-Agent:curl`)
	test(`* | where "_\\message*":"*"`, `* | where "_\\message\*":*`)
	test(`* | where '_\message'*:*`, `* | where "_\\message\*":*`)
	test(`* | where test:composite-pipes | where * | fields level`, `* | where test:composite-pipes | where * | fields level`)
	test(` # My search query
			* 
| 
where test:composite-pipes|where *
|remove level`, `* | where test:composite-pipes | where * | remove level`)

	test(`* | where level:error | where level:*`, `* | where level:error | where level:*`)
}
