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
	// Wildcards must be escaped in the output.
	test(`* | fields "_\\message*"`, `* | fields "_\\message\*"`)
}

func TestParsePipeDelete(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.SeqQLString())
	}

	test("* | delete message,error, level", "* | delete message, error, level")
	test("* | delete level", "* | delete level")
	test("* | delete level", "* | delete level")
	test(`* | delete "_id"`, `* | delete _id`)
	test(`* | delete "_\\message\\_"`, `* | delete "_\\message\\_"`)
	test(`* | delete "_\\message*"`, `* | delete "_\\message\*"`)
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
	test(`* | where test:composite-pipes | delete level | fields level`, `* | where test:composite-pipes | delete level | fields level`)
	test(` # My search query
			* 
| 
where test:composite-pipes|delete level|
fields level`, `* | where test:composite-pipes | delete level | fields level`)
}
