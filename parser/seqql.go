package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ozontech/seq-db/seq"
)

type SeqQLQuery struct {
	Root  *ASTNode
	Pipes []Pipe
}

func (q *SeqQLQuery) SeqQLString() string {
	b := &strings.Builder{}
	q.Root.DumpSeqQL(b)
	for _, p := range q.Pipes {
		b.WriteString(" | ")
		p.DumpSeqQL(b)
	}
	return b.String()
}

func ParseSeqQL(q string, mapping seq.Mapping) (SeqQLQuery, error) {
	lex := newLexer(q)

	lex.Next()
	root, err := parseSeqQLFilter(&lex, mapping, 0)
	if err != nil {
		return SeqQLQuery{}, err
	}

	var pipes []Pipe
	if lex.IsKeyword("|") {
		pipes, err = parsePipes(&lex)
		if err != nil {
			return SeqQLQuery{}, err
		}
	}

	if !lex.IsEnd() {
		panic(fmt.Errorf("BUG: lexer is not end: %+v", lex))
	}

	root, not := propagateNot(root)
	if not {
		root = newNotNode(root)
	}

	return SeqQLQuery{
		Root:  root,
		Pipes: pipes,
	}, nil
}

type lexer struct {
	q string

	Token        string
	tokenQuoted  bool
	SpaceSkipped bool
}

func newLexer(q string) lexer {
	return lexer{
		q:            q,
		Token:        "",
		tokenQuoted:  false,
		SpaceSkipped: false,
	}
}

func (lex *lexer) IsKeyword(token string) bool {
	if lex.tokenQuoted {
		return false
	}
	return strings.EqualFold(lex.Token, token)
}

func (lex *lexer) IsKeywords(tokens ...string) bool {
	if lex.tokenQuoted {
		return false
	}
	for _, t := range tokens {
		if strings.EqualFold(lex.Token, t) {
			return true
		}
	}
	return false
}

func (lex *lexer) IsEnd() bool {
	return lex.q == "" && lex.Token == "" && !lex.tokenQuoted
}

func (lex *lexer) Next() {
	lex.Token = ""
	lex.tokenQuoted = false
	lex.SpaceSkipped = false

again:
	r, size := utf8.DecodeRuneInString(lex.q)
	if r == utf8.RuneError {
		// It is empty string or invalid UTF-8 sequence.
		lex.nextToken(size)
		return
	}

	// Skip spaces.
	for unicode.IsSpace(r) {
		lex.q = lex.q[size:]
		r, size = utf8.DecodeRuneInString(lex.q)
		lex.SpaceSkipped = true
	}

	// Skip comment line.
	if r == '#' {
		n := strings.IndexByte(lex.q[1:], '\n')
		if n == -1 {
			lex.q = ""
		} else {
			lex.q = lex.q[n+1:]
		}
		goto again
	}

	// Decode simple token.
	tokenLen := 0
	for isTokenRune(r) {
		tokenLen += size
		r, size = utf8.DecodeRuneInString(lex.q[tokenLen:])
	}
	if tokenLen > 0 {
		lex.nextToken(tokenLen)
		return
	}

	switch r {
	case '\'', '"':
		out, rem, err := unquotePrefix(lex.q)
		if err != nil {
			lex.nextToken(1)
			return
		}
		lex.Token = out
		lex.q = rem
		lex.tokenQuoted = true
		return
	case '`':
		quotedPrefix, err := strconv.QuotedPrefix(lex.q)
		if err != nil {
			lex.nextToken(1)
			return
		}
		token := quotedPrefix[1 : len(quotedPrefix)-1]
		token = strings.ReplaceAll(token, "*", `\*`)
		lex.Token = token
		lex.q = lex.q[len(quotedPrefix):]
		lex.tokenQuoted = true
		return
	default:
		// Consume only 1 rune.
		lex.nextToken(size)
		return
	}
}

func isTokenRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '.'
}

// unquotePrefix returns unquoted string prefix and remaining string.
// For example, if q is `"payment-api" and level:"info"` then out will be `payment-api` and rem will be ` and level:"info"`.
func unquotePrefix(q string) (out, rem string, _ error) {
	if len(q) < 2 {
		return "", "", strconv.ErrSyntax
	}
	quote := q[0]
	if quote != '"' && quote != '`' && quote != '\'' {
		return "", "", strconv.ErrSyntax
	}

	// Find end of the quoted token.
	end := strings.IndexByte(q[1:], quote)
	if end == -1 {
		return "", "", strconv.ErrSyntax
	}
	end++

	if strings.IndexByte(q[1:end], '\\') == -1 && strings.IndexByte(q[1:end], '\n') == -1 {
		// No need to escape special characters.
		return q[1:end], q[end+1:], nil
	}

	// Unquote 'token' -> token.
	prefix := q[1:]
	b := make([]byte, 0, len(prefix))
	remIdx := 1
	for prefix != "" && prefix[0] != quote {
		ch, _, newTail, err := strconv.UnquoteChar(prefix, quote)
		if err != nil {
			return "", "", err
		}
		b = utf8.AppendRune(b, ch)
		remIdx += len(prefix) - len(newTail)
		prefix = newTail
	}
	remIdx++ // Skip last quote.

	return string(b), q[remIdx:], nil
}

func (lex *lexer) nextToken(size int) {
	lex.Token = lex.q[:size]
	lex.q = lex.q[size:]
}

// parseSeqQLFilter parses SeqQL full text search filters like `"payment-api" and level:"info"` and returns AST node.
func parseSeqQLFilter(lex *lexer, mapping seq.Mapping, depth int) (*ASTNode, error) {
	leftHigh, err := parseSeqQLSubexpr(lex, mapping, depth) // left operand of AND, of high priority
	if err != nil {
		return nil, err
	}
	var leftLow *ASTNode // left operand of OR, of low priority
	for {
		var opKind logicalKind
		switch {
		case lex.IsKeyword("and"):
			opKind = LogicalAnd
		case lex.IsKeyword("or"):
			opKind = LogicalOr
		default:
			if lex.IsEnd() || lex.IsKeyword(")") && depth > 0 || lex.IsKeyword("|") {
				if leftLow != nil && leftHigh != nil {
					return newLogicalNode(LogicalOr, leftLow, leftHigh), nil
				}
				return leftHigh, nil
			}
			return nil, fmt.Errorf("expected 'and', 'or', 'not', got: %q", lex.Token)
		}
		lex.Next()
		right, err := parseSeqQLSubexpr(lex, mapping, depth)
		if err != nil {
			return nil, err
		}
		if opKind == LogicalAnd {
			// leftLow OR leftHigh AND right = leftLow OR (leftHigh AND right)
			// no need to touch leftLow
			leftHigh = newLogicalNode(LogicalAnd, leftHigh, right)
		} else {
			if leftLow == nil {
				// leftHigh can no longer be amended with `AND`
				leftLow = leftHigh
			} else {
				// leftLow OR leftHigh OR right = (leftLow OR leftHigh) OR right
				// just fold
				leftLow = newLogicalNode(LogicalOr, leftLow, leftHigh)
			}
			// it can always be followed by `AND`
			leftHigh = right
		}
	}
}

func parseSeqQLSubexpr(lex *lexer, mapping seq.Mapping, depth int) (*ASTNode, error) {
	if lex.IsEnd() {
		return nil, fmt.Errorf("unexpected end of query")
	}

	if lex.IsKeyword("*") && depth == 0 {
		lex.Next()
		// Query is `*`.
		return &ASTNode{
			Value: &Literal{
				Field: seq.TokenAll,
				Terms: []Term{{Kind: TermSymbol, Data: "*"}},
			},
		}, nil
	}

	if lex.IsKeyword("(") {
		lex.Next()
		expr, err := parseSeqQLFilter(lex, mapping, depth+1)
		if err != nil {
			return nil, err
		}
		if !lex.IsKeyword(")") {
			return nil, fmt.Errorf("missing ')'")
		}
		lex.Next()
		return expr, nil
	}

	if lex.IsKeyword("not") {
		lex.Next()
		child, err := parseSeqQLSubexpr(lex, mapping, depth)
		if err != nil {
			return nil, err
		}
		return newNotNode(child), nil
	}

	tokens, err := parseSeqQLFieldFilter(lex, mapping)
	if err != nil {
		return nil, err
	}
	return buildAndTree(tokens), nil
}

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
		default:
			return nil, fmt.Errorf("unknown pipe: %s", lex.Token)
		}
	}
	return pipes, nil
}

func uniqueTokens(s []string) map[string]struct{} {
	m := make(map[string]struct{})
	for _, v := range s {
		m[v] = struct{}{}
	}
	return m
}
