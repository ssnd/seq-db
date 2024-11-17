package parser

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/seq"
)

var filterStopTokens = []string{"'", `"`, ":", "{", "}", "|", "*", "and", "or", `\`}

func parseSeqQLFieldFilter(lex *lexer, mapping seq.Mapping) ([]Token, error) {
	if lex.IsKeywords(filterStopTokens...) || lex.IsKeywords(rangeStopTokens...) {
		return nil, fmt.Errorf("expected field name, got %q", lex.Token)
	}

	fieldName := lex.Token
	t := indexType(mapping, fieldName)
	if t == seq.TokenizerTypeNoop {
		return nil, fmt.Errorf("field %q is not indexed", fieldName)
	}

	lex.Next()
	if !lex.IsKeyword(":") {
		return nil, fmt.Errorf("missing ':' after %q", fieldName)
	}

	lex.Next()
	if lex.IsKeyword("") {
		return nil, fmt.Errorf("missing filter value for field %q", fieldName)
	}

	caseSensitive := conf.CaseSensitive
	if fieldName == seq.TokenExists {
		caseSensitive = true
	}

	// Parse range filter.
	if lex.IsKeywords("[", "(") {
		r, err := parseSeqQLTokenRange(fieldName, lex, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing range for field %q: %s", fieldName, err)
		}
		return []Token{r}, nil
	}

	if lex.IsKeywords(termStopTokens...) {
		return nil, fmt.Errorf("expected filter value for field %q, got %s", fieldName, lex.Token)
	}

	// Parse fulltext search filter.
	switch t {
	case seq.TokenizerTypeKeyword, seq.TokenizerTypePath:
		tokens, err := parseSeqQLKeyword(fieldName, lex, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing keyword for field %q: %s", fieldName, err)
		}
		return tokens, nil
	case seq.TokenizerTypeText:
		tokens, err := parseSeqQLText(fieldName, lex, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing text for field %q: %s", fieldName, err)
		}
		return tokens, nil
	default:
		panic(fmt.Errorf("BUG: unexpected index type: %d", t))
	}
}

var termStopTokens = []string{"", "[", "]", "(", ")", "'", `"`, ":", "{", "}", "|", "and", "or", `\`, ","}

func parseSeqQLKeyword(field string, lex *lexer, caseSensitive bool) ([]Token, error) {
	if lex.IsKeywords(rangeStopTokens...) {
		return nil, fmt.Errorf("unexpected token %q", lex.Token)
	}

	var terms []Term
	tokenStarts := false
	for ; (!tokenStarts || !lex.SpaceSkipped) && !lex.IsKeywords(termStopTokens...); lex.Next() {
		tokenStarts = true

		if lex.IsKeyword("*") {
			if len(terms) > 0 && terms[len(terms)-1].IsWildcard() {
				return nil, fmt.Errorf("duplicate wildcard")
			}
			terms = append(terms, Term{Kind: TermSymbol, Data: "*"})
			continue
		}

		value := lex.Token
		if !caseSensitive {
			value = strings.ToLower(value)
		}
		terms = append(terms, Term{Kind: TermText, Data: value})
	}

	return []Token{&Literal{Field: field, Terms: terms}}, nil
}

func parseSeqQLText(field string, lex *lexer, sensitive bool) ([]Token, error) {
	var tokens []Token
	current := &Literal{
		Field: field,
		Terms: []Term{},
	}

	tokenStarts := false
	for ; (!tokenStarts || !lex.SpaceSkipped) && !lex.IsKeywords(termStopTokens...); lex.Next() {
		tokenStarts = true

		if lex.IsKeyword("*") {
			current.Terms = append(current.Terms, newSymbolTerm('*'))
			continue
		}

		token := lex.Token
		if token == "" {
			current.Terms = append(current.Terms, newTextTerm(""))
			continue
		}

		startIdx := 0
		for i, r := range token {
			if unicode.IsLetter(r) || unicode.IsNumber(r) {
				// Term is not complete.
				continue
			}

			// Start new literal because of space or other symbols.
			if term := token[startIdx:i]; term != "" {
				current.appendCasedTerm(Term{Kind: TermText, Data: term}, sensitive)
			}
			startIdx = i + 1
			if len(current.Terms) > 0 {
				tokens = append(tokens, current)
			}
			current = &Literal{
				Field: field,
				Terms: []Term{},
			}
		}
		if tail := token[startIdx:]; tail != "" {
			current.appendCasedTerm(Term{Kind: TermText, Data: tail}, sensitive)
		}
	}

	if len(current.Terms) > 0 {
		tokens = append(tokens, current)
		current = nil
	}

	// todo: no tokens, return an error?
	if len(tokens) == 0 {
		tokens = append(tokens, &Literal{
			Field: field,
			Terms: []Term{{Kind: TermText, Data: ""}},
		})
	}
	return tokens, nil
}
