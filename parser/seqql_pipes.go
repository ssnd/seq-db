package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ozontech/seq-db/seq"
)

type Pipe interface {
	DumpSeqQL(*strings.Builder)
}

func parsePipes(lex *lexer) ([]Pipe, error) {
	var pipes []Pipe
	for !lex.IsEnd() {
		if !lex.IsKeyword("|") {
			return nil, fmt.Errorf("expect pipe separator '|', got %s", lex.Token)
		}
		lex.Next()

		switch {
		case lex.IsKeyword("fields"):
			p, err := parsePipeFields(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'fields' pipe: %s", err)
			}
			pipes = append(pipes, p)
		case lex.IsKeyword("delete"):
			p, err := parsePipeDelete(lex)
			if err != nil {
				return nil, fmt.Errorf("parsing 'delete' pipe: %s", err)
			}
			pipes = append(pipes, p)
		case lex.IsKeyword("where"):
			p, err := parsePipeWhere(lex, nil)
			if err != nil {
				return nil, fmt.Errorf("parsing 'where' pipe: %s", err)
			}
			pipes = append(pipes, p)
		default:
			return nil, fmt.Errorf("unknown pipe: %s", lex.Token)
		}
	}
	return pipes, nil
}

type PipeFields struct {
	Fields []string
}

func (f *PipeFields) DumpSeqQL(o *strings.Builder) {
	o.WriteString("fields ")
	for i, field := range f.Fields {
		if i > 0 {
			o.WriteString(", ")
		}
		o.WriteString(quoteTokenIfNeeded(field))
	}
}

func parsePipeFields(lex *lexer) (*PipeFields, error) {
	if !lex.IsKeyword("fields") {
		return nil, fmt.Errorf("missing 'fields' keyword")
	}

	lex.Next()
	var fields []string
	for !lex.IsKeywords("|", "") {
		field, err := parseCompositeTokenReplaceWildcards(lex)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
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
		o.WriteString(quoteTokenIfNeeded(field))
	}
}

func parsePipeDelete(lex *lexer) (*PipeDelete, error) {
	if !lex.IsKeyword("delete") {
		return nil, fmt.Errorf("missing 'fields' keyword, got %q", lex.Token)
	}

	lex.Next()
	var fields []string
	for !lex.IsKeywords("|", "") {
		field, err := parseCompositeTokenReplaceWildcards(lex)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
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

type PipeWhere struct {
	Root *ASTNode
}

func parsePipeWhere(lex *lexer, mapping seq.Mapping) (*PipeWhere, error) {
	if !lex.IsKeyword("where") {
		return nil, fmt.Errorf("missing 'where' keyword, got %q", lex.Token)
	}

	lex.Next()
	root, err := parseSeqQLFilter(lex, mapping, 0)
	if err != nil {
		return nil, err
	}
	return &PipeWhere{Root: root}, nil
}

func (p *PipeWhere) DumpSeqQL(o *strings.Builder) {
	o.WriteString("where ")
	p.Root.DumpSeqQL(o)
}

func quoteTokenIfNeeded(token string) string {
	if !needQuoteToken(token) {
		return token
	}
	return quote(token)
}

// quote returns string with escaped special characters.
func quote(s string) string {
	s = strconv.Quote(s)
	s = strings.ReplaceAll(s, "*", `\*`)
	return s
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

	// Wildcard.
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
		if !isTokenRune(r) && r != '-' {
			return true
		}
	}
	return false
}
