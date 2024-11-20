package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/seq"
)

var filterStopTokens = []string{"'", `"`, ":", "{", "}", "|", "*", "and", "or"}

func parseSeqQLFieldFilter(lex *lexer, mapping seq.Mapping) ([]Token, error) {
	if lex.IsKeywords(filterStopTokens...) || lex.IsKeywords(rangeStopTokens...) {
		return nil, fmt.Errorf("expected field name, got %q", lex.Token)
	}

	fieldName, err := parseCompositeToken(lex, false)
	if err != nil {
		return nil, err
	}
	if fieldName == "" {
		return nil, fmt.Errorf("empty field name")
	}
	t := indexType(mapping, fieldName)
	if t == seq.TokenizerTypeNoop {
		return nil, fmt.Errorf("field %q is not indexed", fieldName)
	}

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

	if lex.IsKeywords(termStopTokens...) {
		return nil, fmt.Errorf("unexpected token %q", lex.Token)
	}

	// Parse fulltext search filter.
	value, err := parseCompositeToken(lex, true)
	if err != nil {
		return nil, err
	}
	switch t {
	case seq.TokenizerTypeKeyword, seq.TokenizerTypePath:
		terms, err := parseSeqQLKeyword(value, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing keyword for field %q: %s", fieldName, err)
		}
		return []Token{&Literal{Field: fieldName, Terms: terms}}, nil
	case seq.TokenizerTypeText:
		tokens, err := parseSeqQLText(fieldName, value, caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("parsing text for field %q: %s", fieldName, err)
		}
		return tokens, nil
	default:
		panic(fmt.Errorf("BUG: unexpected index type: %d", t))
	}
}

var termStopTokens = []string{"", "[", "]", "(", ")", "'", `"`, ":", "{", "}", "|", "and", "or", ","}

func parseCompositeToken(lex *lexer, quoteAsterisks bool) (string, error) {
	if lex.IsKeywords(termStopTokens...) {
		return "", fmt.Errorf("unexpected token %q", lex.Token)
	}
	firstToken := lex.Token
	if quoteAsterisks && lex.IsRawString() {
		// Replace asterisks with in raw strings.
		firstToken = strings.ReplaceAll(firstToken, `*`, `\*`)
	}
	lex.Next()
	if lex.SpaceSkipped || lex.IsKeywords(termStopTokens...) {
		return firstToken, nil
	}

	b := strings.Builder{}
	b.WriteString(firstToken)
	for ; (!lex.SpaceSkipped) && !lex.IsKeywords(termStopTokens...); lex.Next() {
		token := lex.Token
		if quoteAsterisks && lex.IsRawString() {
			// Replace asterisks with in raw strings.
			token = strings.ReplaceAll(token, `*`, `\*`)
		}
		b.WriteString(token)
	}
	return b.String(), nil
}

func parseSeqQLKeyword(token string, caseSensitive bool) ([]Term, error) {
	if token == "" {
		return []Term{newTextTerm("")}, nil
	}
	var terms []Term
	current := Term{Kind: TermText}

	for token != "" {
		r, size := utf8.DecodeRuneInString(token)
		if r == '*' {
			if current.Data != "" {
				terms = append(terms, newCasedTextTerm(current.Data, caseSensitive))
			}
			current = Term{Kind: TermText}
			terms = append(terms, newSymbolTerm('*'))
			token = token[1:]
			continue
		}
		if strings.HasPrefix(token, "\\*") {
			current.Data += "*"
			token = token[2:]
			continue
		}
		current.Data += string(r)
		token = token[size:]
	}
	if current.Data != "" {
		terms = append(terms, newCasedTextTerm(current.Data, caseSensitive))
	}

	return terms, nil
}

func parseSeqQLText(field string, token string, sensitive bool) ([]Token, error) {
	if token == "" {
		return []Token{&Literal{Field: field, Terms: []Term{newTextTerm("")}}}, nil
	}
	var tokens []Token
	current := &Literal{Field: field}

	term := Term{Kind: TermText}
	for token != "" {
		r, size := utf8.DecodeRuneInString(token)
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' {
			term.Data += string(r)
			token = token[size:]
			continue
		}
		// Unescape wildcard.
		if strings.HasPrefix(token, "\\*") {
			term.Data += "*"
			token = token[2:]
			continue
		}

		// Term is done.
		if term.Data != "" {
			current.appendCasedTerm(term, sensitive)
			term = Term{Kind: TermText}
		}
		if r == '*' {
			current.Terms = append(current.Terms, newSymbolTerm('*'))
			token = token[1:]
			continue
		}

		// This is separator character like ':', '&', ' ', emoji, etc.
		// So create new literal.

		if len(current.Terms) != 0 {
			tokens = append(tokens, current)
			current = &Literal{Field: field}
		}
		token = token[size:]
	}
	if term.Data != "" {
		current.appendCasedTerm(term, sensitive)
		term = Term{Kind: TermText}
	}

	if current != nil && len(current.Terms) > 0 {
		tokens = append(tokens, current)
		current = nil
	}

	if len(tokens) == 0 {
		// There are no tokens to search, return an empty filter.
		tokens = append(tokens, &Literal{
			Field: field,
			Terms: []Term{{Kind: TermText, Data: ""}},
		})
	}
	return tokens, nil
}
