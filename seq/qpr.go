package seq

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/valyala/fastrand"

	"github.com/ozontech/seq-db/metric"
)

type DocsOrder uint8

const (
	DocsOrderDesc DocsOrder = 0
	DocsOrderAsc  DocsOrder = 1
)

func (o DocsOrder) IsDesc() bool {
	return o == DocsOrderDesc
}

func (o DocsOrder) IsReverse() bool {
	return o == DocsOrderAsc
}

type IDSource struct {
	ID     ID
	Source uint64
	Hint   string
}

func (id *IDSource) Equal(check IDSource) bool {
	return id.ID.Equal(check.ID) && id.Source == check.Source
}

func (id *IDSource) MarshalBinary(dst []byte) []byte {
	dst = be.AppendUint64(dst, uint64(id.ID.MID))
	dst = be.AppendUint64(dst, uint64(id.ID.RID))
	dst = be.AppendUint64(dst, id.Source)
	dst = be.AppendUint16(dst, uint16(len(id.Hint)))
	dst = append(dst, id.Hint...)
	return dst
}

func (id *IDSource) UnmarshalBinary(src []byte) ([]byte, error) {
	if len(src) < 26 {
		return src, fmt.Errorf("invalid ID source length; want: 26, got %d", len(src))
	}
	id.ID.MID = MID(be.Uint64(src))
	src = src[8:]
	id.ID.RID = RID(be.Uint64(src))
	src = src[8:]
	id.Source = be.Uint64(src)
	src = src[8:]
	hintLen := be.Uint16(src)
	src = src[2:]
	id.Hint = string(src[:hintLen])
	src = src[hintLen:]
	return src, nil
}

type IDSources []IDSource

func (p IDSources) Len() int           { return len(p) }
func (p IDSources) Less(i, j int) bool { return Less(p[i].ID, p[j].ID) }
func (p IDSources) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p IDSources) IDs() []ID {
	ids := make([]ID, len(p))
	for i, id := range p {
		ids[i] = id.ID
	}
	return ids
}

func (p IDSources) ApplyHint(hint string) {
	for i := range p {
		p[i].Hint = hint
	}
}

type ErrorSource struct {
	ErrStr string
	Source uint64
}

func (e *ErrorSource) MarshalBinary(dst []byte) []byte {
	dst = be.AppendUint64(dst, uint64(len(e.ErrStr)))
	dst = append(dst, e.ErrStr...)
	dst = be.AppendUint64(dst, e.Source)
	return dst
}

func (e *ErrorSource) UnmarshalBinary(src []byte) ([]byte, error) {
	n := be.Uint64(src)
	src = src[8:]
	e.ErrStr = string(src[:n])
	src = src[n:]
	e.Source = be.Uint64(src)
	src = src[8:]
	return src, nil
}

// QPR query partial result, stores intermediate result of running query e.g. result from only one fraction or particular store
// TODO: remove single Agg when n-agg support in proxy is deployed
type QPR struct {
	IDs       IDSources
	Histogram map[MID]uint64
	Aggs      []QPRHistogram
	Total     uint64
	Errors    []ErrorSource
}

func (q *QPR) Aggregate(args []AggregateArgs) []AggregationResult {
	allAggregations := make([]AggregationResult, len(q.Aggs))
	for i, agg := range q.Aggs {
		allAggregations[i] = agg.Aggregate(args[i])
	}
	return allAggregations
}

func (q *QPR) CombineErrors() string {
	x := make([]byte, 0)
	for k, err := range q.Errors {
		if k > 5 {
			break
		}
		x = append(x, err.ErrStr...)
	}

	return string(x)
}

var be = binary.BigEndian

const qprBinVersion = 1

func (q *QPR) MarshalBinary(dst []byte) []byte {
	dst = append(dst, qprBinVersion)

	dst = be.AppendUint64(dst, uint64(len(q.IDs)))
	for _, id := range q.IDs {
		dst = id.MarshalBinary(dst)
	}

	dst = be.AppendUint64(dst, uint64(len(q.Histogram)))
	for mid, hist := range q.Histogram {
		dst = be.AppendUint64(dst, uint64(mid))
		dst = be.AppendUint64(dst, hist)
	}

	dst = be.AppendUint64(dst, uint64(len(q.Aggs)))
	for _, agg := range q.Aggs {
		dst = agg.MarshalBinary(dst)
	}

	dst = be.AppendUint64(dst, q.Total)

	dst = be.AppendUint64(dst, uint64(len(q.Errors)))
	for _, e := range q.Errors {
		dst = e.MarshalBinary(dst)
	}
	return dst
}

func (q *QPR) UnmarshalBinary(src []byte) ([]byte, error) {
	if len(src) < 41 {
		return nil, fmt.Errorf("invalid QPR format; want %d bytes, got %d", 41, len(src))
	}

	version := src[0]
	src = src[1:]
	if version != qprBinVersion {
		return nil, fmt.Errorf("invalid QPR version %d; want %d", version, qprBinVersion)
	}

	n := be.Uint64(src)
	src = src[8:]
	q.IDs = slices.Grow(q.IDs[:0], int(n))
	for i := 0; i < int(n); i++ {
		id := IDSource{}
		tail, err := id.UnmarshalBinary(src)
		if err != nil {
			return nil, fmt.Errorf("invalid IDSource at pos %d: %v", i, err)
		}
		q.IDs = append(q.IDs, id)
		src = tail
	}

	n = be.Uint64(src)
	src = src[8:]
	if len(q.Histogram) < int(n) {
		q.Histogram = make(map[MID]uint64, n)
	} else {
		clear(q.Histogram)
	}
	for i := 0; i < int(n); i++ {
		mid := be.Uint64(src)
		src = src[8:]
		v := be.Uint64(src)
		src = src[8:]
		q.Histogram[MID(mid)] = v
	}

	n = be.Uint64(src)
	src = src[8:]
	q.Aggs = slices.Grow(q.Aggs[:0], int(n))
	for i := 0; i < int(n); i++ {
		agg := QPRHistogram{}
		tail, err := agg.UnmarshalBinary(src)
		if err != nil {
			return nil, fmt.Errorf("invalid QPRHistogram at pos %d: %v", i, err)
		}
		src = tail
		q.Aggs = append(q.Aggs, agg)
	}

	q.Total = be.Uint64(src)
	src = src[8:]

	n = be.Uint64(src)
	src = src[8:]
	for i := 0; i < int(n); i++ {
		var e ErrorSource
		tail, err := e.UnmarshalBinary(src)
		if err != nil {
			return nil, fmt.Errorf("invalid ErrorSource at pos %d: %v", i, err)
		}
		src = tail
		q.Errors = append(q.Errors, e)
	}

	return src, nil
}

type AggFunc byte

const (
	AggFuncCount = iota
	AggFuncSum
	AggFuncMin
	AggFuncMax
	AggFuncAvg
	AggFuncQuantile
	AggFuncUnique
)

type QPRHistogram struct {
	HistogramByToken map[string]*AggregationHistogram
	NotExists        int64
}

type AggregationBucket struct {
	Name      string
	Value     float64
	Quantiles []float64
	NotExists int64
}

type AggregationResult struct {
	Buckets   []AggregationBucket
	NotExists int64
}

type AggregateArgs struct {
	Func      AggFunc
	Quantiles []float64
}

func (q *QPRHistogram) Aggregate(args AggregateArgs) AggregationResult {
	buckets := make([]AggregationBucket, 0, len(q.HistogramByToken))
	for token, hist := range q.HistogramByToken {
		buckets = append(buckets, q.getAggBucket(token, hist, args))
	}

	sortBuckets(args.Func, buckets)

	return AggregationResult{
		Buckets:   buckets,
		NotExists: q.NotExists,
	}
}

func sortBuckets(aggFunc AggFunc, buckets []AggregationBucket) {
	sortByValueDescNameAsc := func(left, right AggregationBucket) int {
		return cmp.Or(
			cmp.Compare(right.Value, left.Value),
			cmp.Compare(left.Name, right.Name),
		)
	}
	sortByNameAscValueDesc := func(left, right AggregationBucket) int {
		return cmp.Or(
			cmp.Compare(left.Name, right.Name),
			cmp.Compare(right.Value, left.Value),
		)
	}
	sortByValueNameAsc := func(left, right AggregationBucket) int {
		return cmp.Or(
			cmp.Compare(left.Value, right.Value),
			cmp.Compare(left.Name, right.Name),
		)
	}

	sortFunc := sortByValueDescNameAsc
	if aggFunc == AggFuncMin {
		// Sort the MIN aggregation result in ascending order.
		sortFunc = sortByValueNameAsc
	}
	if aggFunc == AggFuncQuantile {
		// Sort the QUANTILE aggregation result by name ASC, then by value DESC.
		sortFunc = sortByNameAscValueDesc
	}
	slices.SortFunc(buckets, sortFunc)
}

func (q *QPRHistogram) getAggBucket(token string, hist *AggregationHistogram, args AggregateArgs) AggregationBucket {
	var value float64
	var quantiles []float64

	switch args.Func {
	case AggFuncCount, AggFuncUnique:
		value = float64(hist.Total)
	case AggFuncSum:
		value = hist.Sum
	case AggFuncMin:
		value = hist.Min
	case AggFuncMax:
		value = hist.Max
	case AggFuncAvg:
		if hist.Total != 0 {
			value = hist.Sum / float64(hist.Total)
		}
	case AggFuncQuantile:
		if len(args.Quantiles) == 0 {
			panic(fmt.Errorf("BUG: empty quantiles"))
		}
		quantiles = make([]float64, 0, len(args.Quantiles))
		for _, q := range args.Quantiles {
			quantiles = append(quantiles, hist.Quantile(q))
		}
		value = quantiles[0]
	default:
		panic(fmt.Errorf("unimplemented aggregation func"))
	}

	if hist.Total == 0 && args.Func != AggFuncCount && args.Func != AggFuncUnique {
		value = math.NaN()
	}

	return AggregationBucket{
		Name:      token,
		Value:     value,
		Quantiles: quantiles,
		NotExists: hist.NotExists,
	}
}

func (q *QPRHistogram) Merge(agg QPRHistogram) {
	if q.HistogramByToken == nil {
		q.HistogramByToken = make(map[string]*AggregationHistogram, len(agg.HistogramByToken))
	}

	q.NotExists += agg.NotExists

	for k, v := range agg.HistogramByToken {
		if q.HistogramByToken[k] == nil {
			q.HistogramByToken[k] = NewAggregationHistogram()
		}
		q.HistogramByToken[k].Merge(v)
	}
}

func (q *QPRHistogram) MarshalBinary(dst []byte) []byte {
	dst = be.AppendUint64(dst, uint64(len(q.HistogramByToken)))
	for token, hist := range q.HistogramByToken {
		dst = be.AppendUint64(dst, uint64(len(token)))
		dst = append(dst, token...)
		dst = hist.MarshalBinary(dst)
	}
	dst = be.AppendUint64(dst, uint64(q.NotExists))
	return dst
}

func (q *QPRHistogram) UnmarshalBinary(src []byte) ([]byte, error) {
	if len(src) < 16 {
		return nil, fmt.Errorf("src too short to unmarshal QPRHistogram, want at least 16 bytes, got %d", len(src))
	}

	aggs := be.Uint64(src)
	src = src[8:]
	if len(q.HistogramByToken) < int(aggs) {
		q.HistogramByToken = make(map[string]*AggregationHistogram, aggs)
	} else {
		clear(q.HistogramByToken)
	}
	hists := make([]AggregationHistogram, aggs)
	for i := 0; i < int(aggs); i++ {
		n := be.Uint64(src)
		src = src[8:]
		token := string(src[:n])
		src = src[n:]

		hist := &hists[i]
		tail, err := hist.UnmarshalBinary(src)
		if err != nil {
			return nil, err
		}
		src = tail

		q.HistogramByToken[token] = hist
	}

	q.NotExists = int64(be.Uint64(src))
	src = src[8:]

	return src, nil
}

// AggregationHistogram is a histogram that is used for aggregations.
// Implements reservoir sampling algorithm.
type AggregationHistogram struct {
	rng fastrand.RNG

	Min float64
	Max float64
	Sum float64
	// Total is the number of inserted values.
	Total int64
	// NotExists is the number of values without a token.
	NotExists int64
	Samples   []float64
}

func NewAggregationHistogram() *AggregationHistogram {
	h := &AggregationHistogram{
		Min: math.MaxInt64,
		Max: math.MinInt64,
	}
	// Fixed seed to have same result on the same input
	h.rng.Seed(73)
	return h
}

// Quantile calculates the quantile value of the histogram.
// The argument should be in [0, 1] range.
//
// The implementation is taken and adapted from github.com/valyala/histogram.
func (h *AggregationHistogram) Quantile(quantile float64) float64 {
	if quantile < 0 || quantile > 1 {
		// Must be checked in seqproxy
		panic(fmt.Errorf("BUG: invalid quantile: %f", quantile))
	}

	if len(h.Samples) == 0 {
		return math.NaN()
	}
	if quantile == 1 {
		return h.Max
	}
	if quantile == 0 {
		return h.Min
	}

	slices.Sort(h.Samples)
	index := int(float64(len(h.Samples)-1)*quantile + 0.5) // +0.5 to round up value
	return h.Samples[index]
}

func (h *AggregationHistogram) Merge(hist *AggregationHistogram) {
	h.NotExists += hist.NotExists

	if hist.Total == 0 {
		return
	}

	if h.Total == 0 {
		h.Min = hist.Min
		h.Max = hist.Max
	} else {
		h.Min = min(h.Min, hist.Min)
		h.Max = max(h.Max, hist.Max)
	}

	h.Sum += hist.Sum
	h.Total += hist.Total
	for _, v := range hist.Samples {
		h.InsertSample(v)
	}
}

func (h *AggregationHistogram) InsertNTimes(num float64, cnt int64) {
	if h.Total == 0 {
		h.Min = num
		h.Max = num
	} else {
		h.Min = min(h.Min, num)
		h.Max = max(h.Max, num)
	}
	h.Sum += num * float64(cnt)
	h.Total += cnt
}

func (h *AggregationHistogram) InsertSampleNTimes(sample float64, cnt int64) {
	for i := int64(0); i < cnt; i++ {
		h.InsertSample(sample)
	}
}

const maxHistogramSamples = 8096

func (h *AggregationHistogram) InsertSample(num float64) {
	if len(h.Samples) < maxHistogramSamples {
		h.Samples = append(h.Samples, num)
	} else {
		h.Samples[h.rng.Uint32()%maxHistogramSamples] = num
	}
}

func (h *AggregationHistogram) MarshalBinary(dst []byte) []byte {
	dst = be.AppendUint64(dst, math.Float64bits(h.Min))
	dst = be.AppendUint64(dst, math.Float64bits(h.Max))
	dst = be.AppendUint64(dst, math.Float64bits(h.Sum))
	dst = be.AppendUint64(dst, uint64(h.Total))
	dst = be.AppendUint64(dst, uint64(h.NotExists))

	dst = be.AppendUint64(dst, uint64(len(h.Samples)))
	for _, v := range h.Samples {
		dst = be.AppendUint64(dst, math.Float64bits(v))
	}
	return dst
}

func (h *AggregationHistogram) UnmarshalBinary(src []byte) ([]byte, error) {
	if len(src) < 48 {
		return src, fmt.Errorf("histogram size too low")
	}
	h.Min = math.Float64frombits(be.Uint64(src))
	src = src[8:]
	h.Max = math.Float64frombits(be.Uint64(src))
	src = src[8:]
	h.Sum = math.Float64frombits(be.Uint64(src))
	src = src[8:]
	h.Total = int64(be.Uint64(src))
	src = src[8:]
	h.NotExists = int64(be.Uint64(src))
	src = src[8:]

	n := be.Uint64(src)
	src = src[8:]
	h.Samples = slices.Grow(h.Samples[:0], int(n))
	for i := 0; i < int(n); i++ {
		v := math.Float64frombits(be.Uint64(src))
		h.Samples = append(h.Samples, v)
		src = src[8:]
	}

	return src, nil
}

func MergeQPRs(dst *QPR, qprs []*QPR, limit int, histInterval MID, order DocsOrder) {
	idsCount := 0
	for _, qpr := range qprs {
		idsCount += len(qpr.IDs)
	}

	dst.IDs = slices.Grow(dst.IDs, idsCount)

	for _, qpr := range qprs {
		dst.Total += qpr.Total
		if qpr.Histogram != nil && dst.Histogram == nil {
			dst.Histogram = make(map[MID]uint64)
		}
		for time, count := range qpr.Histogram {
			dst.Histogram[time] += count
		}

		if qpr.Aggs != nil && dst.Aggs == nil {
			dst.Aggs = make([]QPRHistogram, len(qpr.Aggs))
		}
		for i := range qpr.Aggs {
			dst.Aggs[i].Merge(qpr.Aggs[i])
		}

		dst.IDs = append(dst.IDs, qpr.IDs...)
		dst.Errors = append(dst.Errors, qpr.Errors...)
	}

	if order.IsReverse() {
		sort.Sort(dst.IDs)
	} else {
		// it is not a bug: regular order is descending
		sort.Sort(sort.Reverse(dst.IDs))
	}

	ids, repetitionsCount := removeRepetitionsAdvanced(dst.IDs, dst.Histogram, histInterval)
	metric.RepetitionsDocsTotal.Add(float64(repetitionsCount))

	// count only for queries with total
	if dst.Total > 0 {
		dst.Total -= repetitionsCount
	}

	l := min(len(ids), limit)
	dst.IDs = ids[:l]
}

// removes repetitions from both ids and histogram
func removeRepetitionsAdvanced(ids IDSources, histogram map[MID]uint64, histInterval MID) (IDSources, uint64) {
	if len(ids) == 0 {
		return ids, 0
	}

	removeCount := 0

	lastID := ids[0]
	for i := 1; i < len(ids); i++ {
		if lastID.ID != ids[i].ID {
			lastID = ids[i]
			ids[i-removeCount] = ids[i]
		} else {
			removeCount++

			if histInterval > 0 {
				removeHistogramRepetition(lastID, histogram, histInterval)
			}
		}
	}

	return ids[:len(ids)-removeCount], uint64(removeCount)
}

// remove repetition from histogram
func removeHistogramRepetition(repetition IDSource, histogram map[MID]uint64, histInterval MID) {
	bucket := repetition.ID.MID
	bucket -= bucket % histInterval
	histogram[bucket]--
}
