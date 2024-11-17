package parser

import (
	"fmt"
	"strings"
)

type Range struct {
	Field       string
	From        Term
	To          Term
	IncludeFrom bool
	IncludeTo   bool
}

func (n *Range) Dump(builder *strings.Builder) {
	builder.WriteString(n.Field)
	builder.WriteString(`:`)
	if n.IncludeFrom {
		builder.WriteByte('[')
	} else {
		builder.WriteByte('{')
	}
	n.From.Dump(builder)
	builder.WriteString(" TO ")
	n.To.Dump(builder)
	if n.IncludeTo {
		builder.WriteByte(']')
	} else {
		builder.WriteByte('}')
	}
}

func (n *Range) DumpSeqQL(b *strings.Builder) {
	b.WriteString(n.Field)
	b.WriteString(`:`)
	if n.IncludeFrom {
		b.WriteByte('[')
	} else {
		b.WriteByte('(')
	}
	n.From.DumpSeqQL(b)
	b.WriteString(", ")
	n.To.DumpSeqQL(b)
	if n.IncludeTo {
		b.WriteByte(']')
	} else {
		b.WriteByte(')')
	}
}

var rangeStopTokens = []string{"[", "]", "(", ")", "'", `"`, ":", "{", "}", "|", "and", "or", `\`}

func parseSeqQLTokenRange(field string, lex *lexer, sensitive bool) (*Range, error) {
	r := &Range{Field: field}
	if !lex.IsKeywords("(", "[") {
		return r, fmt.Errorf("range start not found")
	}

	r.IncludeFrom = lex.Token == "["

	lex.Next()
	if lex.IsKeywords(rangeStopTokens...) {
		return r, fmt.Errorf("unexpected token %q", lex.Token)
	}
	if err := parseRangeTerm(&r.From, lex, sensitive); err != nil {
		return r, err
	}

	lex.Next()
	if !lex.IsKeywords(",") {
		return r, fmt.Errorf("expected ',' keyword, got %q", lex.Token)
	}

	lex.Next()
	if lex.IsKeywords(rangeStopTokens...) {
		return r, fmt.Errorf("unexpected token %q", lex.Token)
	}
	if err := parseRangeTerm(&r.To, lex, sensitive); err != nil {
		return r, err
	}

	lex.Next()
	if !lex.IsKeywords(")", "]") {
		return r, fmt.Errorf("range end not found")
	}
	r.IncludeTo = lex.Token == "]"
	lex.Next()
	return r, nil
}

func parseRangeTerm(term *Term, lex *lexer, sensitive bool) error {
	term.Kind = TermText
	token := lex.Token
	if !sensitive {
		token = strings.ToLower(token)
	}
	term.Data = token
	if lex.IsKeyword("*") {
		term.Kind = TermSymbol
	}
	return nil
}
