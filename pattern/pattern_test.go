package pattern

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/parser"
)

type testTokenProvider struct {
	ordered  *simpleTokenProvider
	shuffled *simpleTokenProvider
}

func newTestTokenProvider(data []string) testTokenProvider {
	if len(data) < 2 {
		return testTokenProvider{ // empty provider
			ordered:  &simpleTokenProvider{data: data, ordered: true},
			shuffled: &simpleTokenProvider{data: data, ordered: false},
		}
	}

	// prepare shuffled
	shuffled := simpleTokenProvider{
		data:    make([]string, len(data)),
		ordered: true,
	}
	copy(shuffled.data, data)
	for i := 0; i < 100 && shuffled.ordered; i++ {
		rand.Shuffle(len(shuffled.data), func(i int, j int) {
			shuffled.data[i], shuffled.data[j] = shuffled.data[j], shuffled.data[i]
		})
		shuffled.ordered = isOrdered(shuffled.data)
	}

	ordered := simpleTokenProvider{
		data:    make([]string, len(data)),
		ordered: true,
	}
	copy(ordered.data, data)
	sort.Strings(ordered.data)
	ordered.data = uniq(ordered.data)

	return testTokenProvider{
		ordered:  &ordered,
		shuffled: &shuffled,
	}
}

func uniq(data []string) []string {
	var prev string
	i := 0
	for _, v := range data {
		if v == prev {
			continue
		}
		prev = v
		data[i] = v
		i++
	}
	return data[:i]
}

func isOrdered(data []string) bool {
	for i := 0; i < len(data)-1; i++ {
		if data[i] > data[i+1] {
			return false
		}
	}
	return true
}

type simpleTokenProvider struct {
	data    []string
	ordered bool
}

func (tp *simpleTokenProvider) GetToken(i uint32) []byte {
	return []byte(tp.data[i-1])
}

func (tp *simpleTokenProvider) FirstTID() uint32 {
	return 1
}

func (tp *simpleTokenProvider) LastTID() uint32 {
	return uint32(len(tp.data))
}

func (tp *simpleTokenProvider) Ordered() bool {
	return tp.ordered
}

func searchAll(t *testing.T, tp testTokenProvider, req string, expect []string) {
	sort.Strings(expect)
	assert.False(t, tp.shuffled.Ordered(), "data is sorted")
	search(t, tp.shuffled, req, expect)

	assert.True(t, tp.ordered.Ordered(), "data is not sorted")
	search(t, tp.ordered, req, uniq(expect))
}

func search(t *testing.T, tp *simpleTokenProvider, req string, expect []string) {
	searchType := "full"
	if tp.Ordered() {
		searchType = "narrow"
	}

	token, err := parser.ParseSingleTokenForTests("m", req)
	require.NoError(t, err)
	s := newSearcher(token, tp)

	res := []string{}
	for i := s.firstTID(); i <= s.lastTID(); i++ {
		val := tp.GetToken(i)
		if s.check(val) {
			res = append(res, string(val))
		}
	}
	sort.Strings(res)

	assert.Equal(t, expect, res, "%s search request %q failed", searchType, req)
}

func TestPatternSimple(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"ab",
		"abc",
		"bcfg",
		"bd",
		"efg",
		"lka",
		"lkk",
		"x",
		"x",
		"zaaa",
	})

	searchAll(t, tp, "b*", []string{"bcfg", "bd"})
	searchAll(t, tp, "f*", []string{})
	searchAll(t, tp, "efg", []string{"efg"})
	searchAll(t, tp, "ef", []string{})
	searchAll(t, tp, "lk*", []string{"lka", "lkk"})
	searchAll(t, tp, "a*", []string{"ab", "abc"})
	searchAll(t, tp, "z*", []string{"zaaa"})
	searchAll(t, tp, "ab", []string{"ab"})
	searchAll(t, tp, "aa", []string{})
	searchAll(t, tp, "zz", []string{})
	searchAll(t, tp, "zaaa", []string{"zaaa"})
	searchAll(t, tp, "b*g", []string{"bcfg"})
	searchAll(t, tp, "b*d", []string{"bd"})
	searchAll(t, tp, "z*a", []string{"zaaa"})
	searchAll(t, tp, "x", []string{"x", "x"})
}

func TestPatternPrefix(t *testing.T) {
	data := []string{
		"a",
		"aa",
		"aba",
		"abc",
		"abc",
		"aca",
		"acb",
		"acba",
		"acbb",
		"acbccc",
		"acbz",
		"acdd",
		"ace",
		"acff",
		"ad",
		"az",
	}
	tp := newTestTokenProvider(data)

	searchAll(t, tp, "a*", data)
	searchAll(t, tp, "ab*", []string{"aba", "abc", "abc"})
	searchAll(t, tp, "ac*", []string{"aca", "acb", "acba", "acbb", "acbccc", "acbz", "acdd", "ace", "acff"})
	searchAll(t, tp, "acb*", []string{"acb", "acba", "acbb", "acbccc", "acbz"})
	searchAll(t, tp, "acb", []string{"acb"})
	searchAll(t, tp, "acba*", []string{"acba"})
	searchAll(t, tp, "acc*", []string{})
	searchAll(t, tp, "acc", []string{})
	searchAll(t, tp, "acz*", []string{})
}

func TestPatternEmpty(t *testing.T) {
	tp := newTestTokenProvider([]string{})

	searchAll(t, tp, "a", []string{})
	searchAll(t, tp, "abc", []string{})
	searchAll(t, tp, "*", []string{})
}

func TestPatternSingle(t *testing.T) {
	tp := newTestTokenProvider([]string{"abacaba"})

	searchAll(t, tp, "abacaba", []string{"abacaba"})
	searchAll(t, tp, "*", []string{"abacaba"})
	searchAll(t, tp, "a*", []string{"abacaba"})
	searchAll(t, tp, "a", []string{})
	searchAll(t, tp, "abc", []string{})
}

func TestPatternSuffix(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"abc",
		"acd",
		"acdc:suf",
		"acdd",
		"acdd",
		"acdfg:suf",
		"acg",
		"add:suf",
	})

	searchAll(t, tp, `acd*\:suf`, []string{`acdc:suf`, `acdfg:suf`})
	searchAll(t, tp, `acd*`, []string{`acd`, `acdc:suf`, `acdd`, `acdd`, `acdfg:suf`})
	searchAll(t, tp, `ac*\:suf`, []string{`acdc:suf`, `acdfg:suf`})
	searchAll(t, tp, `ac*f`, []string{`acdc:suf`, `acdfg:suf`})
	searchAll(t, tp, `ac*d`, []string{`acd`, `acdd`, `acdd`})
	searchAll(t, tp, `acdc\:suf`, []string{`acdc:suf`})
	searchAll(t, tp, `*\:suf`, []string{`acdc:suf`, `acdfg:suf`, `add:suf`})
}

func TestPatternSuffix2(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"aba",
		"abac",
		"abacaba",
		"caba",
	})

	searchAll(t, tp, "*", []string{"aba", "abac", "abacaba", "caba"})
	searchAll(t, tp, "aba*", []string{"aba", "abac", "abacaba"})
	searchAll(t, tp, "aba*aba", []string{"abacaba"})
	searchAll(t, tp, "abac*aba", []string{"abacaba"})
	searchAll(t, tp, "aba*caba", []string{"abacaba"})
	searchAll(t, tp, "abac*caba", []string{})
	searchAll(t, tp, "*caba", []string{"abacaba", "caba"})
}

func TestPatternMiddle(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"a:b:a",
		"aba",
		"abacaba",
		"abracadabra",
		"some:Data:hey",
	})

	searchAll(t, tp, `ab*c*ba`, []string{`abacaba`})
	searchAll(t, tp, `a*b*a`, []string{`a:b:a`, `aba`, `abacaba`, `abracadabra`})
	searchAll(t, tp, `a*c*a`, []string{`abacaba`, `abracadabra`})
	searchAll(t, tp, `a*\:b\:*a`, []string{`a:b:a`})
	searchAll(t, tp, `*acada*`, []string{`abracadabra`})
	searchAll(t, tp, `*aba*`, []string{`aba`, `abacaba`})
	searchAll(t, tp, `*ac*ca*`, []string{})
}

func TestRange(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"1",
		"34",
		"12",
		"-3",
		"15",
		"44",
		"45",
		"46",
		"120481",
		"-12",
		"-15",
	})

	searchAll(t, tp, "[2 to 16]", []string{"12", "15"})
	searchAll(t, tp, "[1 to 1]", []string{"1"})
	searchAll(t, tp, "{1 to 1}", []string{})
	searchAll(t, tp, "{44 to 46}", []string{"45"})
	searchAll(t, tp, "[44 to 46}", []string{"44", "45"})
	searchAll(t, tp, "{44 to 46]", []string{"45", "46"})
	searchAll(t, tp, "[44 to 46]", []string{"44", "45", "46"})
	searchAll(t, tp, "[-16 to -10]", []string{"-12", "-15"})

	// result is sorted as strings in test function. actual result is not sorted
	searchAll(t, tp, "[1 to 34]", []string{"1", "12", "15", "34"})
	searchAll(t, tp, "[16 to 2]", []string{})
}

func TestRangeNumberWildcard(t *testing.T) {
	maxInt64 := strconv.Itoa(math.MaxInt64)
	minInt64 := strconv.Itoa(math.MinInt64)

	tp := newTestTokenProvider([]string{
		"-4",
		"-8",
		"13",
		"3",
		"402.0",
		"Inf",
		"+Inf",
		"-Inf",
		"NaN",
		maxInt64,
		minInt64,
		"0",
		"сорок два",
		"",
		" ",
		"a",
	})

	searchAll(t, tp, "[* to -8]", []string{"-8", minInt64})
	searchAll(t, tp, "{* to -8]", []string{"-8", minInt64})
	searchAll(t, tp, "[* to -8}", []string{minInt64})
	searchAll(t, tp, "[* to 3]", []string{"-4", "-8", minInt64, "0", "3"})
	searchAll(t, tp, "[* to 3}", []string{"-4", "-8", minInt64, "0"})
	searchAll(t, tp, "[13 to *]", []string{"13", "402.0", maxInt64})
	searchAll(t, tp, "{13 to *]", []string{"402.0", maxInt64})
	searchAll(t, tp, "[402 to *]", []string{"402.0", maxInt64})
	searchAll(t, tp, "[402 to *}", []string{"402.0", maxInt64})
	searchAll(t, tp, "{402 to *]", []string{maxInt64})
	searchAll(t, tp, "[* to *]", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, tp, "{* to *]", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, tp, "[* to *}", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, tp, "{* to *}", []string{"-4", "-8", minInt64, "0", "13", "3", "402.0", maxInt64})
	searchAll(t, tp, "[402.0 to 402.0]", []string{"402.0"})
}

func TestRangeText(t *testing.T) {

	tp := newTestTokenProvider([]string{
		"ab",
		"abc",
		"bcfg",
		"bd",
		"efg",
		"lka",
		"lkk",
		"x",
		"zaaa",
	})

	searchAll(t, tp, "[bd to efg]", []string{"bd", "efg"})
	searchAll(t, tp, "[bd to efg}", []string{"bd"})
	searchAll(t, tp, "{bd to efg}", []string{})
	searchAll(t, tp, "{bd to efg]", []string{"efg"})
	searchAll(t, tp, "[bb to efg]", []string{"bcfg", "bd", "efg"})
	searchAll(t, tp, "{bb to efg]", []string{"bcfg", "bd", "efg"})
	searchAll(t, tp, "[bb to efh]", []string{"bcfg", "bd", "efg"})
	searchAll(t, tp, "[bb to efh}", []string{"bcfg", "bd", "efg"})

	searchAll(t, tp, "[* to ab]", []string{"ab"})
	searchAll(t, tp, "{* to ab]", []string{"ab"})
	searchAll(t, tp, "[* to ab}", []string{})
	searchAll(t, tp, "[* to bd]", []string{"ab", "abc", "bcfg", "bd"})
	searchAll(t, tp, "[* to bd}", []string{"ab", "abc", "bcfg"})
	searchAll(t, tp, "[lkk to *]", []string{"lkk", "x", "zaaa"})
	searchAll(t, tp, "{lkk to *]", []string{"x", "zaaa"})
	searchAll(t, tp, "[zaaa to *]", []string{"zaaa"})
	searchAll(t, tp, "[zaaa to *}", []string{"zaaa"})
	searchAll(t, tp, "{zaaa to *]", []string{})

}

func TestPatternSymbols(t *testing.T) {
	tp := newTestTokenProvider([]string{
		"*",
		"**",
		"****",
		"val=*",
		"val=***",
	})

	searchAll(t, tp, `\*`, []string{"*"})
	searchAll(t, tp, `\**`, []string{"*", "**", "****"})
	searchAll(t, tp, `\*\*`, []string{"**"})
	searchAll(t, tp, `\*\**`, []string{"**", "****"})
	searchAll(t, tp, `val=*`, []string{"val=*", "val=***"})
	searchAll(t, tp, `val=\*`, []string{"val=*"})
	searchAll(t, tp, `val=\**`, []string{"val=*", "val=***"})
	searchAll(t, tp, `val=\*\*\*`, []string{"val=***"})
}
