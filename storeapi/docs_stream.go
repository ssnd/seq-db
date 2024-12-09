package storeapi

import (
	"context"
	"errors"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/fetch"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"go.uber.org/zap"
)

const initChunkSize = 1000

type streamDocsBatch struct {
	docs [][]byte
	err  error
}

type docsStream struct {
	ctx       context.Context
	ids       seq.IDSources
	totalIDs  int
	fetcher   *fetch.Fetcher
	fracs     fracmanager.FracsList
	out       chan streamDocsBatch
	docsBatch [][]byte
}

func (d *docsStream) Next() ([]byte, error) {
	if len(d.docsBatch) == 0 {
		batch := <-d.out
		if batch.err != nil {
			return nil, batch.err
		}
		d.docsBatch = batch.docs
	}

	doc := d.docsBatch[0]
	d.docsBatch = d.docsBatch[1:]

	return doc, nil
}

func (d *docsStream) nextBatch(chunkSize int) (b streamDocsBatch) {
	var chunk seq.IDSources
	l := min(len(d.ids), chunkSize)
	chunk, d.ids = d.ids[:l], d.ids[l:]

	if len(chunk) == 0 {
		b.err = errors.New("no more ids for fetch")
	} else {
		b.docs, b.err = d.fetcher.FetchDocs(d.ctx, d.fracs, chunk)
	}

	return b
}

func (d *docsStream) batchLoader() {
	chunkSize := initChunkSize

	for {
		select {
		case <-d.ctx.Done():
			return
		default:
			batch := d.nextBatch(chunkSize)

			if !d.send(batch) {
				return
			}

			if batch.err != nil {
				return
			}

			chunkSize = d.calcChunkSize(batch.docs, chunkSize)
		}
	}
}

func (d *docsStream) send(b streamDocsBatch) bool {
	for {
		select {
		case <-d.ctx.Done():
			return false
		case d.out <- b:
			return true
		}
	}
}

func (d *docsStream) calcChunkSize(docs [][]byte, prevChunkSize int) int {
	batchSize := 0
	for _, doc := range docs {
		batchSize += len(doc)
	}
	if batchSize == 0 {
		return prevChunkSize
	}

	avgDocSize := batchSize / len(docs)
	newChunkSize := conf.MaxFetchSizeBytes / avgDocSize
	if newChunkSize != prevChunkSize {
		logger.Debug(
			"fetch chunk recalculated",
			zap.Int("total_len", d.totalIDs),
			zap.Int("prev_chunk_size", prevChunkSize),
			zap.Int("new_chunk_size", newChunkSize),
			zap.String("batch_size", datasize.ByteSize(batchSize).HumanReadable()),
		)
	}

	return newChunkSize
}

func newDocsStream(ctx context.Context, ids seq.IDSources, fetcher *fetch.Fetcher, fracs fracmanager.FracsList) *docsStream {
	d := docsStream{
		ctx:      ctx,
		ids:      ids,
		totalIDs: len(ids),
		fetcher:  fetcher,
		fracs:    fracs,
		out:      make(chan streamDocsBatch, 1), // buffered chan to async load the next batch while we are sending the prev batch to the client
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		d.batchLoader()
	}()

	go func() {
		wg.Wait()
		close(d.out)
	}()

	return &d
}
