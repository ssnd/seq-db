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
		require.Equal(t, expected, query.String())
	}

	test("* | fields  message,error, level", "* | fields message, error, level")
	test("* | fields level", "* | fields level")
	test("* | fields level", "* | fields level")
	test(`* | fields "_id"`, `* | fields _id`)
	test(`* | fields "_\\message\\_"`, `* | fields "_\\message\\_"`)
	test(`* | fields "_\\message*"`, `* | fields "_\\message*"`)
}

func TestParsePipeDelete(t *testing.T) {
	test := func(q, expected string) {
		t.Helper()
		query, err := ParseSeqQL(q, nil)
		require.NoError(t, err)
		require.Equal(t, expected, query.String())
	}

	test("* | delete message,error, level", "* | delete message, error, level")
	test("* | delete level", "* | delete level")
	test("* | delete level", "* | delete level")
	test(`* | delete "_id"`, `* | delete _id`)
	test(`* | delete "_\\message\\_"`, `* | delete "_\\message\\_"`)
	test(`* | delete "_\\message*"`, `* | delete "_\\message*"`)
}
