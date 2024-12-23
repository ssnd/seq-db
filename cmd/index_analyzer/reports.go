package main

import (
	"fmt"
	"math"
	"sort"
)

type Stats struct {
	docsCount int

	fields     int
	tokenCount int
	tokenSize  int

	uniqTokenValuesCount int

	uniqTokenCountSum int
	uniqTokenSizeSum  int

	lidsUniqCount int
	lidsCount     int

	lidsHist   map[int]int
	tokensHist map[int]int
}

func newStats(
	tokensUniq map[string]map[string]int,
	tokensValuesUniq map[string]int,
	tokens [][]byte,
	docsCount, lidsUniqCount, lidsTotalCount int,
) Stats {
	uniqTokenSize := 0
	uniqTokenCount := 0
	lidsHist := make(map[int]int)
	tokensHist := make(map[int]int)

	for _, fieldToken := range tokensUniq {
		fieldTokenCount := len(fieldToken)
		uniqTokenCount += fieldTokenCount
		tokensHist[int(math.Pow(2, math.Ceil(math.Log2(float64(fieldTokenCount)))))]++
		for t, cnt := range fieldToken {
			uniqTokenSize += len(t)
			lidsHist[int(math.Pow(2, math.Ceil(math.Log2(float64(cnt)))))]++
		}
	}

	tokenSize := 0
	for _, t := range tokens {
		tokenSize += len(t)
	}

	return Stats{
		docsCount: docsCount,

		tokenCount: len(tokens),
		tokenSize:  tokenSize,

		uniqTokenValuesCount: len(tokensValuesUniq),

		uniqTokenCountSum: uniqTokenCount,
		uniqTokenSizeSum:  uniqTokenSize,

		fields: len(tokensUniq),

		lidsUniqCount: lidsUniqCount,
		lidsCount:     lidsTotalCount,

		lidsHist:   lidsHist,
		tokensHist: tokensHist,
	}

}

func printTokensStat(stats []Stats) {
	docsTotalSum := 0
	tokensSizeSum := 0
	tokensCountSum := 0

	fmt.Printf(
		"%2s,%7s,%9s,%7s,%16s,%16s,%16s,%16s,%16s,%17s,%17s\n",
		"N", "docs", "docsSum", "fields", "tokens", "tokensSum", "uniqTokensSum", "tokenSize", "tokenSizeSum", "uniqTokenSizeSum", "uniqTokenValues",
	)

	for i, s := range stats {
		docsTotalSum += s.docsCount
		tokensSizeSum += s.tokenSize
		tokensCountSum += s.tokenCount

		fmt.Printf("%2d,%7d,%9d,%7d,%16d,%16d,%16d,%16d,%16d,%17d,%17d\n",
			i+1,

			s.docsCount,
			docsTotalSum,

			s.fields,

			s.tokenCount,
			tokensCountSum,
			s.uniqTokenCountSum,

			s.tokenSize,
			tokensSizeSum,
			s.uniqTokenSizeSum,
			s.uniqTokenValuesCount,
		)
	}
}

func printTokensHistStat(stats []Stats) {
	hist := map[int]map[int]int{}
	for i, s := range stats {
		for bucket, v := range s.tokensHist {
			if _, ok := hist[bucket]; !ok {
				hist[bucket] = map[int]int{}
			}
			hist[bucket][i] = v
		}
	}

	printHist(hist, len(stats))
}

func printLIDsHistStat(stats []Stats) {
	hist := map[int]map[int]int{}
	for i, s := range stats {
		for bucket, v := range s.lidsHist {
			if _, ok := hist[bucket]; !ok {
				hist[bucket] = map[int]int{}
			}
			hist[bucket][i] = v
		}
	}

	printHist(hist, len(stats))
}

func printHist(hist map[int]map[int]int, rows int) {
	buckets := []int{}
	for bucket := range hist {
		buckets = append(buckets, bucket)
	}
	sort.Ints(buckets)

	fmt.Printf("%3s", "N")
	for _, b := range buckets {
		fmt.Printf(",%9d", b)
	}
	fmt.Println()

	for i := 0; i < rows; i++ {
		fmt.Printf("%3d", i+1)
		for _, b := range buckets {
			fmt.Printf(",%9d", hist[b][i])
		}
		fmt.Println()
	}
}

func printUniqLIDsStats(stats []Stats) {
	fmt.Printf(
		"%2s,%7s,%16s,%16s\n",
		"N", "docs", "lidsCount", "lidsUniqCount",
	)
	for i, s := range stats {
		fmt.Printf("%2d,%7d,%16d,%16d\n",
			i+1,
			s.docsCount,
			s.lidsCount,
			s.lidsUniqCount,
		)
	}
}
