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

	for i := 0; i < len(s); i++ {
		if !isAscii[s[i]] {
			return toLowerUnicode(s)
		}

		s[i] = toLowerMap[s[i]]
	}

	return s
}

func toLowerUnicode(s []byte) []byte {
	// nolint
	return bytes.Map(unicode.ToLower, s)
}

var (
	// toLowerMap maps upper ASCII symbols to lower. It is safe to use it on utf8 bytes (i > utf8.RuneSelf),
	// since ASCII symbol cannot be a part of other utf8 encoded symbol https://en.wikipedia.org/wiki/UTF-8#Description
	toLowerMap [256]byte

	// isAscii      returns true for given byte `b` if b < utf8.RuneSelf
	isAscii [256]bool

	// isUpperAscii returns true for given byte `b` if 'A' <= b && b <= 'Z'
	isUpperAscii [256]bool

	// isTextToken  returns true for given byte `b` if that byte should be parsed by tokenizer (for more information refer to initIsTextToken).
	//
	// 128 bytes is enough for them, but we use 256 to skip bound checks when we use isTextToken[byte(i)].
	isTextToken [256]bool
)

func init() {
	initUpperToLowerMap()
	initIsAscii()
	initIsUpperAscii()
	initIsTextToken()
}

func initUpperToLowerMap() {
	for i := 0; i < 256; i++ {
		toLowerMap[i] = byte(i)

		if 'A' <= i && i <= 'Z' {
			toLowerMap[i] += 'a' - 'A'
		}
	}
}

func initIsAscii() {
	for i := 0; i < utf8.RuneSelf; i++ {
		isAscii[i] = true
	}
}

func initIsUpperAscii() {
	for i := 'A'; i <= 'Z'; i++ {
		isUpperAscii[i] = true
	}
}

func initIsTextToken() {
	for i := 0; i < 256; i++ {
		// letters and digits
		if 'a' <= i && i <= 'z' || 'A' <= i && i <= 'Z' || '0' <= i && i <= '9' {
			isTextToken[i] = true
		}

		// other characters
		if i == '_' || i == '*' {
			isTextToken[i] = true
		}
	}
}
