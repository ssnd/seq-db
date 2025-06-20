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
	Aggregations      []AggQuery
	HistogramInterval seq.MID
	WithDocs          bool
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
		HistogramInterval: int64(r.HistogramInterval),
		Retention:         durationpb.New(r.Retention),
		WithDocs:          r.WithDocs,
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
	ID     string
	Size   int
	Offset int
	Order  seq.DocsOrder
}

type FetchAsyncSearchResultResponse struct {
	Status     fracmanager.AsyncSearchStatus
	QPR        seq.QPR
	CanceledAt time.Time

	StartedAt time.Time
	ExpiresAt time.Time

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
		Size:     int32(r.Size),
		Offset:   int32(r.Offset),
	}

	fracsDone := 0
	fracsInQueue := 0
	histInterval := seq.MID(0)
	pr := FetchAsyncSearchResultResponse{}
	mergeStoreResp := func(sr *storeapi.FetchAsyncSearchResultResponse, replica string) {
		pr.DiskUsage += sr.DiskUsage
		fracsInQueue += int(sr.FracsQueue)
		fracsDone += int(sr.FracsDone)

		histInterval = seq.MID(sr.HistogramInterval)

		ss := sr.Status.MustAsyncSearchStatus()
		pr.Status = mergeAsyncSearchStatus(pr.Status, ss)

		for _, errStr := range sr.GetResponse().GetErrors() {
			pr.QPR.Errors = append(pr.QPR.Errors, seq.ErrorSource{
				ErrStr: errStr,
				Source: si.sourceByClient[replica],
			})
		}

		t := sr.ExpiresAt.AsTime()
		if pr.ExpiresAt.IsZero() || pr.ExpiresAt.After(t) {
			pr.ExpiresAt = t
		}

		qpr := responseToQPR(sr.Response, si.sourceByClient[replica], false)
		seq.MergeQPRs(&pr.QPR, []*seq.QPR{qpr}, r.Size, histInterval, r.Order)
	}

	var aggQueries []seq.AggregateArgs
	anyResponse := false
	for _, shard := range searchStores.Shards {
		for _, replica := range shard {
			storeResp, err := si.clients[replica].FetchAsyncSearchResult(ctx, &req)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					continue
				}
				return FetchAsyncSearchResultResponse{}, err
			}
			anyResponse = true
			mergeStoreResp(storeResp, replica)
			if len(aggQueries) == 0 {
				for _, agg := range storeResp.Aggs {
					aggQueries = append(aggQueries, seq.AggregateArgs{
						Func:      agg.Func.MustAggFunc(),
						Quantiles: agg.Quantiles,
					})
				}
			}
			break
		}
	}
	if !anyResponse {
		return FetchAsyncSearchResultResponse{}, status.Error(codes.NotFound, "async search result not found")
	}

	pr.Progress = (float64(fracsDone) + float64(fracsInQueue)) / float64(fracsDone)
	if pr.Status == fracmanager.AsyncSearchStatusDone {
		pr.Progress = 1
	}
	pr.AggResult = pr.QPR.Aggregate(aggQueries)
	return pr, nil
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
