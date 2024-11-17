package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type PipeFields struct {
	Fields []string
}

func (f *PipeFields) DumpSeqQL(o *strings.Builder) {
	o.WriteString("fields ")
	for i, field := range f.Fields {
		if i > 0 {
			o.WriteString(", ")
		}
		o.WriteString(quoteToken(field))
	}
}

func parsePipeFields(lex *lexer) (*PipeFields, error) {
	if !lex.IsKeyword("fields") {
		return nil, fmt.Errorf("missing 'fields' keyword")
	}

	lex.Next()
	var fields []string
	for !lex.IsKeywords("|", "") {
		if lex.IsKeywords(filterStopTokens...) {
			return nil, fmt.Errorf("unexpected '%s'", lex.Token)
		}
		fields = append(fields, lex.Token)
		lex.Next()
		if lex.IsKeyword(",") {
			lex.Next()
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("empty list")
	}

	return &PipeFields{
		Fields: fields,
	}, nil
}

type PipeDelete struct {
	Fields []string
}

func (p *PipeDelete) DumpSeqQL(o *strings.Builder) {
	o.WriteString("delete ")
	for i, field := range p.Fields {
		if i > 0 {
			o.WriteString(", ")
		}
		o.WriteString(quoteToken(field))
	}
}

func parsePipeDelete(lex *lexer) (*PipeDelete, error) {
	if !lex.IsKeyword("delete") {
		return nil, fmt.Errorf("missing 'fields' keyword, got %q", lex.Token)
	}

	lex.Next()
	var fields []string
	for !lex.IsKeywords("|", "") {
		if lex.IsKeywords(filterStopTokens...) {
			return nil, fmt.Errorf("unexpected '%s'", lex.Token)
		}
		fields = append(fields, lex.Token)
		lex.Next()
		if lex.IsKeyword(",") {
			lex.Next()
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("empty list")
	}

	return &PipeDelete{
		Fields: fields,
	}, nil
}

func quoteToken(token string) string {
	if !needQuoteToken(token) {
		return token
	}
	return strconv.Quote(token)
}

var reservedKeywords = uniqueTokens([]string{
	// End of query.
	"",
	// Range filter and parentheses for grouping filters.
	"(", ")",
	// Range filter.
	"[", "]",
	// Range border separators.
	",",

	// Logical operators.
	"or",
	"and",
	"not",

	// Wildcards.
	"*",

	// Field delimiter.
	":",

	// Pipe separator.
	"|",
})

func needQuoteToken(s string) bool {
	if _, ok := reservedKeywords[strings.ToLower(s)]; ok {
		return true
	}
	for _, r := range s {
		if !isTokenRune(r) {
			return true
		}
	}
	return false
}
