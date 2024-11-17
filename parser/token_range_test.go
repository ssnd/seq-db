package parser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

func TestSeqQLParseRange(t *testing.T) {
	r := require.New(t)
	got, err := ParseSeqQL(`level:(1, *]`, seq.TestMapping)
	r.NoError(err)

	expected := SeqQLQuery{
		Root: &ASTNode{
			Value: &Range{
				Field:       "level",
				From:        newTextTerm("1"),
				To:          newSymbolTerm('*'),
				IncludeFrom: false,
				IncludeTo:   true,
			},
		},
	}
	r.NotNil(got.Root.Value.(*Range))
	r.Equal(expected, got)
}
