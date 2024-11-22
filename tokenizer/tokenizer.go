package tokenizer

import (
	"bytes"
	"unicode"
	"unicode/utf8"

	"github.com/ozontech/seq-db/frac"
)

type Tokenizer interface {
	Tokenize(tokens []frac.MetaToken, key, value []byte, maxLength int) []frac.MetaToken
}

func toLowerIfCaseInsensitive(isCaseSensitive bool, x []byte) []byte {
	if isCaseSensitive {
		return x
	}

	return toLowerTryInplace(x)
}

// toLowerTryInplace tries to lowercase given []byte inplace (without allocations)
// but if utf-8 is encountered, fallbacks to bytes.Map which returns new []byte
func toLowerTryInplace(s []byte) []byte {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return toLowerUnicode(s)
		}

		hasUpper = hasUpper || upperCaseMap[s[i]]
	}

	if hasUpper {
		for i := 0; i < len(s); i++ {
			if !upperCaseMap[s[i]] {
				continue
			}

			s[i] += 'a' - 'A'
		}
	}

	return s
}

func toLowerUnicode(s []byte) []byte {
	// nolint
	return bytes.Map(unicode.ToLower, s)
}

// allTextTokenChars contains text token symbols that are ASCII.
// 128 bytes is enough for them, but we use 256 to skip bound checks when we use allTextTokenChars[byte(i)].
var allTextTokenChars = [256]bool{
	'0': true, '1': true, '2': true, '3': true, '4': true, '5': true, '6': true, '7': true, '8': true, '9': true,

	'a': true, 'b': true, 'c': true, 'd': true, 'e': true, 'f': true, 'g': true, 'h': true, 'i': true, 'j': true,
	'k': true, 'l': true, 'm': true, 'n': true, 'o': true, 'p': true, 'q': true, 'r': true, 's': true, 't': true,
	'u': true, 'v': true, 'w': true, 'x': true, 'y': true, 'z': true,
	'A': true, 'B': true, 'C': true, 'D': true, 'E': true, 'F': true, 'G': true, 'H': true, 'I': true, 'J': true,
	'K': true, 'L': true, 'M': true, 'N': true, 'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true,
	'U': true, 'V': true, 'W': true, 'X': true, 'Y': true, 'Z': true,

	'_': true, '*': true,
}

var upperCaseMap = [256]bool{
	'A': true, 'B': true, 'C': true, 'D': true, 'E': true, 'F': true, 'G': true, 'H': true, 'I': true, 'J': true,
	'K': true, 'L': true, 'M': true, 'N': true, 'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true,
	'U': true, 'V': true, 'W': true, 'X': true, 'Y': true, 'Z': true,
}
