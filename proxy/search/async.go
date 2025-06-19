package search

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type AsyncRequest struct {
	Retention         time.Duration
	Query             string
	From              time.Time
	To                time.Time
	Order             seq.DocsOrder
	Aggregations      []AggQuery
	HistogramInterval seq.MID
}

type AsyncResponse struct {
	ID string
}

func (si *Ingestor) StartAsyncSearch(ctx context.Context, r AsyncRequest) (AsyncResponse, error) {
	requestID := uuid.New().String()

	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	req := storeapi.StartAsyncSearchRequest{
		SearchId:          requestID,
		Query:             r.Query,
		From:              r.From.UnixMilli(),
		To:                r.To.UnixMilli(),
		Aggs:              convertToAggsQuery(r.Aggregations),
		Order:             storeapi.MustProtoOrder(r.Order),
		HistogramInterval: int64(r.HistogramInterval),
		Retention:         durationpb.New(r.Retention),
	}
	for i, shard := range searchStores.Shards {
		var err error

		// todo shuffle
		for _, replica := range shard {
			_, err = si.clients[replica].StartAsyncSearch(ctx, &req)
			if err != nil {
				logger.Error("Can't start async search",
					zap.String("replica", replica), zap.Error(err))
				continue
			}
			break
		}
		if err != nil {
			return AsyncResponse{}, fmt.Errorf("starting search in shard=%d: %s", i, err)
		}
	}

	return AsyncResponse{ID: requestID}, nil
}

type FetchAsyncSearchResultRequest struct {
	ID       string
	WithDocs bool
	Size     int
	Offset   int
}

type FetchAsyncSearchResultResponse struct {
	Status     fracmanager.AsyncSearchStatus
	QPR        seq.QPR
	CanceledAt time.Time
	Error      string

	StartedAt time.Time
	ExpiredAt time.Time

	Progress  float64
	DiskUsage uint64

	AggResult []seq.AggregationResult
}

func (si *Ingestor) FetchAsyncSearchResult(ctx context.Context, r FetchAsyncSearchResultRequest) (FetchAsyncSearchResultResponse, error) {
	searchStores := si.config.HotStores
	if si.config.HotReadStores != nil && len(si.config.HotReadStores.Shards) > 0 {
		searchStores = si.config.HotReadStores
	}

	req := storeapi.FetchAsyncSearchResultRequest{
		SearchId: r.ID,
		WithDocs: r.WithDocs,
		Size:     int32(r.Size),
		Offset:   int32(r.Offset),
	}

	fracsDone := 0
	fracsInQueue := 0
	var aggQueries []seq.AggregateArgs
	anyResponse := false
	histInterval := seq.MID(0)
	resp := FetchAsyncSearchResultResponse{}
	for _, shard := range searchStores.Shards {
		var storeResp *storeapi.FetchAsyncSearchResultResponse
		var err error
		var replica string
		for _, replica = range shard {
			storeResp, err = si.clients[replica].FetchAsyncSearchResult(ctx, &req)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					continue
				}
				return FetchAsyncSearchResultResponse{}, err
			}
			break
		}
		if err != nil {
			logger.Warn("shard does not have async search request")
			continue
		}
		anyResponse = true

		resp.DiskUsage += storeResp.DiskUsage
		fracsInQueue += int(storeResp.FracsQueue)
		fracsDone += int(storeResp.FracsDone)

		histInterval = seq.MID(storeResp.HistogramInterval)
		order := storeResp.Order.MustDocsOrder()

		ss := storeResp.Status.MustAsyncSearchStatus()
		resp.Status = mergeAsyncSearchStatus(resp.Status, ss)

		t := storeResp.ExpiredAt.AsTime()
		if resp.ExpiredAt.IsZero() || resp.ExpiredAt.After(t) {
			resp.ExpiredAt = t
		}

		if len(aggQueries) == 0 {
			for _, agg := range storeResp.Aggs {
				aggQueries = append(aggQueries, seq.AggregateArgs{
					Func:      agg.Func.MustAggFunc(),
					Quantiles: agg.Quantiles,
				})
			}
		}
		qpr := responseToQPR(storeResp.Response, si.sourceByClient[replica], false)
		seq.MergeQPRs(&resp.QPR, []*seq.QPR{qpr}, r.Size, histInterval, order)
	}
	if !anyResponse {
		return FetchAsyncSearchResultResponse{}, status.Error(codes.NotFound, "async search result not found")
	}

	resp.Progress = (float64(fracsDone) + float64(fracsInQueue)) / float64(fracsDone)
	if resp.Status == fracmanager.AsyncSearchStatusDone {
		resp.Progress = 1
	}

	resp.AggResult = resp.QPR.Aggregate(aggQueries)
	return resp, nil
}

func mergeAsyncSearchStatus(a, b fracmanager.AsyncSearchStatus) fracmanager.AsyncSearchStatus {
	statusWeight := []fracmanager.AsyncSearchStatus{
		0:                                       0,
		fracmanager.AsyncSearchStatusDone:       1,
		fracmanager.AsyncSearchStatusInProgress: 2,
		fracmanager.AsyncSearchStatusCanceled:   3,
		fracmanager.AsyncSearchStatusError:      4,
	}
	weightA := statusWeight[a]
	weightB := statusWeight[b]
	if weightA >= weightB {
		return a
	}
	return b
}
