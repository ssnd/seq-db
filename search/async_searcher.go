package search

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/zstd"
	"go.uber.org/zap"
)

type Searcher interface {
	Search(ctx context.Context, fracs []frac.Fraction, params Params) ([]*seq.QPR, []*Stats, error)
}

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type AsyncSearcher struct {
	config AsyncSearcherConfig

	mp MappingProvider

	fracManager *fracmanager.FracManager
	searcher    Searcher

	requestsMu sync.RWMutex
	requests   map[string]asyncSearchInfo

	rateLimit chan struct{}
}

type AsyncSearcherConfig struct {
	DataDir     string
	Parallelism int
}

func MustStartAsyncSearcher(config AsyncSearcherConfig, mp MappingProvider, m *fracmanager.FracManager, searcher Searcher) *AsyncSearcher {
	if config.DataDir == "" {
		logger.Fatal("can't start async searcher: DataDir is empty")
	}

	if err := os.MkdirAll(config.DataDir, 0o777); err != nil {
		panic(err)
	}

	asyncSearches, err := loadAsyncSearches(config.DataDir)
	if err != nil {
		logger.Fatal("failed to load previous async searches", zap.Error(err))
	}

	parallelism := config.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}

	as := &AsyncSearcher{
		config:      config,
		mp:          mp,
		fracManager: m,
		searcher:    searcher,
		requestsMu:  sync.RWMutex{},
		requests:    asyncSearches,
		rateLimit:   make(chan struct{}, parallelism),
	}

	notProcessedIDs := notProcessedTasks(asyncSearches)
	for _, id := range notProcessedIDs {
		go as.processRequest(id)
	}

	return as
}

type AsyncSearchRequest struct {
	ID        string
	Params    Params
	Query     string
	Retention time.Duration
}

type fracInfo struct {
	Name string
}

type asyncSearchInfo struct {
	Done       bool
	Request    AsyncSearchRequest
	Fractions  []fracInfo
	Expiration time.Time
	StartTime  time.Time
}

const asyncSearchFileExtension = ".info"

func (as *AsyncSearcher) StartSearch(r AsyncSearchRequest) error {
	ast, err := parser.ParseSeqQL(r.Query, as.mp.GetMapping())
	if err != nil {
		return err
	}
	r.Params.AST = ast.Root

	fracs, err := as.fracManager.SelectFracsInRange(r.Params.From, r.Params.To)
	if err != nil {
		return err
	}
	if len(fracs) == 0 {
		return nil
	}

	fracsToSearch := make([]fracInfo, 0, len(fracs))
	for _, f := range fracs {
		fracsToSearch = append(fracsToSearch, fracInfo{Name: f.Info().Name()})
	}

	const asyncSearchTmpFileExtension = "._info"
	fnameTmp := path.Join(as.config.DataDir, r.ID+asyncSearchTmpFileExtension)
	fname := path.Join(as.config.DataDir, r.ID+asyncSearchFileExtension)

	metaFile, err := os.OpenFile(fnameTmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		logger.Fatal("can't create async search info file", zap.Error(err))
	}
	defer metaFile.Close()

	now := time.Now()
	info := asyncSearchInfo{
		Done:       false,
		Request:    r,
		Fractions:  fracsToSearch,
		Expiration: now.Add(r.Retention),
		StartTime:  now,
	}
	if err := json.NewEncoder(metaFile).Encode(info); err != nil {
		logger.Fatal("can't encode async request to info file", zap.Error(err))
	}

	if err := metaFile.Sync(); err != nil {
		logger.Fatal("can't sync info file", zap.Error(err))
	}

	if err := os.Rename(fnameTmp, fname); err != nil {
		logger.Fatal("can't rename async search info file", zap.Error(err))
	}

	as.requestsMu.Lock()
	as.requests[r.ID] = info
	as.requestsMu.Unlock()

	go as.processRequest(r.ID)

	return nil
}

func (as *AsyncSearcher) processRequest(asyncSearchID string) {
	as.rateLimit <- struct{}{}
	defer func() { <-as.rateLimit }()

	if err := as.doSearch(asyncSearchID); err != nil {
		logger.Fatal("async search failed", zap.Error(err))
	}
}

func (as *AsyncSearcher) doSearch(id string) error {
	qprPaths, err := as.loadQPRPaths(id)
	if err != nil {
		return fmt.Errorf("loading processed fracs: %s", err)
	}

	processedFracs := make(map[string]struct{})
	for _, qprPath := range qprPaths {
		fracName, err := fracNameFromQPRPath(qprPath)
		if err != nil {
			return err
		}
		processedFracs[fracName] = struct{}{}
	}

	as.requestsMu.RLock()
	state, ok := as.requests[id]
	as.requestsMu.RUnlock()
	if !ok {
		panic(fmt.Errorf("BUG: can't find async search request for id %s", id))
	}

	if len(processedFracs) == len(state.Fractions) {
		state.Done = true
		as.requestsMu.Lock()
		as.requests[id] = state
		as.requestsMu.Unlock()
		return nil
	}

	// AST can be nil in case of restarts.
	if state.Request.Params.AST == nil {
		ast, err := parser.ParseSeqQL(state.Request.Query, as.mp.GetMapping())
		if err != nil {
			panic(fmt.Errorf("BUG: search query must be valid: %s", err))
		}
		state.Request.Params.AST = ast.Root
	}

	r := state.Request
	fracsInRange, err := as.fracManager.SelectFracsInRange(r.Params.From, r.Params.To)
	if err != nil {
		return err
	}

	for _, fracInfo := range state.Fractions {
		if _, ok := processedFracs[fracInfo.Name]; ok {
			continue
		}
		f := fracsInRange.FindByName(fracInfo.Name)

		// todo send batch of fracs?
		if err := as.processFrac(f, state.Request); err != nil {
			return fmt.Errorf("processing fraction %s: %s", fracInfo.Name, err)
		}
	}

	return nil
}

var compressBufPool = sync.Pool{
	New: func() interface{} {
		return new(bytespool.Buffer)
	},
}

func (as *AsyncSearcher) processFrac(f frac.Fraction, r AsyncSearchRequest) error {
	qprs, _, err := as.searcher.Search(context.Background(), []frac.Fraction{f}, r.Params)
	if err != nil {
		return err
	}
	qpr := qprs[0]
	// qpr can be nil if fraction suicided.
	if qpr == nil {
		return nil
	}

	qprRaw, err := json.Marshal(qpr)
	if err != nil {
		return err
	}

	// zstd uses level 3 as the default value, which has optimal compression ratio and speed.
	// Please, create an issue if you want to change this value via configuration.
	const compressionLevel = 3

	compressedQPR := compressBufPool.Get().(*bytespool.Buffer)
	compressedQPR.B = zstd.CompressLevel(qprRaw, compressedQPR.B, compressionLevel)
	defer compressBufPool.Put(compressedQPR)

	qprFilePrefix := path.Join(as.config.DataDir, r.ID+"."+f.Info().Name()) // <data-dir>/<request_id>.<frac_name>
	const tmpQPRExtension = "._qpr"
	qprTmpFileName := qprFilePrefix + tmpQPRExtension
	qprResultFileName := qprFilePrefix + qprExtension

	out, err := os.OpenFile(qprTmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := out.Write(compressedQPR.B); err != nil {
		return fmt.Errorf("writing compressed QPR: %s", err)
	}

	if err := out.Sync(); err != nil {
		return fmt.Errorf("syncing compressed QPR: %s", err)
	}

	if err := os.Rename(qprTmpFileName, qprResultFileName); err != nil {
		return err
	}

	return nil
}

func fracNameFromQPRPath(qprPath string) (string, error) {
	filename := path.Base(qprPath)
	parts := strings.Split(filename, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("unknown qpr filename format")
	}
	// example path: a087f43f-40f6-49ae-8743-2fd7bef1dfd5.seq-db-01JQRFHKSBZTY5NSZ937WV987B.qpr
	// parts[0] is request ID
	// parts[1] is fraction name
	// parts[2] is extension
	return parts[1], nil
}

const qprExtension = ".qpr"

func (as *AsyncSearcher) loadQPRPaths(id string) ([]string, error) {
	pattern := path.Join(as.config.DataDir, id+"*"+qprExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func loadAsyncSearches(dataDir string) (map[string]asyncSearchInfo, error) {
	requests := make(map[string]asyncSearchInfo)

	pattern := path.Join(dataDir, "*"+asyncSearchFileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	for _, fpath := range files {
		filename := path.Base(fpath)

		ext := path.Ext(filename)
		if ext != asyncSearchFileExtension {
			logger.Fatal("unknown file", zap.String("filename", filename))
		}

		requestID := filename[:len(filename)-len(ext)]
		b, err := os.ReadFile(fpath)
		if err != nil {
			return nil, err
		}
		var req asyncSearchInfo
		if err := json.Unmarshal(b, &req); err != nil {
			logger.Error("can't load async search request", zap.String("filename", filename), zap.Error(err))
			continue
		}
		// It is difficult to marshal/unmarshal AST, so set it to nil and parse it later.
		req.Request.Params.AST = nil
		requests[requestID] = req
	}

	return requests, nil
}

func notProcessedTasks(tasks map[string]asyncSearchInfo) []string {
	var toProcess []string

	//nolint:gocritic // rangeValCopy (each iteration copies 216 bytes) – it's ok here
	for id, task := range tasks {
		if !task.Done {
			toProcess = append(toProcess, id)
		}
	}

	// Sort tasks by start time.
	slices.SortFunc(toProcess, func(a, b string) int {
		left := tasks[a].StartTime
		right := tasks[b].StartTime
		if left.After(right) {
			return -1
		}
		if left.Equal(right) {
			return 0
		}
		return 1
	})

	return toProcess
}

type FetchSearchResultRequest struct {
	ID string
}

type FetchSearchResultResponse struct {
	QPR        seq.QPR
	Expiration time.Time
	Done       bool

	// Stuff that needed seq-db proxy to complete async search response.
	AggQueries   []AggQuery
	HistInterval uint64
	Order        seq.DocsOrder
}

func (as *AsyncSearcher) FetchSearchResult(r FetchSearchResultRequest) (FetchSearchResultResponse, bool) {
	as.requestsMu.RLock()
	info, ok := as.requests[r.ID]
	as.requestsMu.RUnlock()
	if !ok {
		return FetchSearchResultResponse{}, false
	}

	qprPaths, err := as.loadQPRPaths(r.ID)
	if err != nil {
		logger.Error("can't load async search result", zap.String("id", r.ID), zap.Error(err))
	}

	qpr := seq.QPR{}
	for _, qprPath := range qprPaths {
		compressedQPR, err := os.ReadFile(qprPath)
		if err != nil {
			logger.Error("can't load async search result", zap.String("id", r.ID), zap.Error(err))
		}
		qprRaw, err := zstd.Decompress(compressedQPR, nil)
		if err != nil {
			logger.Error("can't load decompress search result", zap.String("id", r.ID), zap.Error(err))
		}

		var tmp seq.QPR
		if err := json.Unmarshal(qprRaw, &tmp); err != nil {
			logger.Error("can't unmarshal search result", zap.String("id", r.ID), zap.Error(err))
		}

		seq.MergeQPRs(&qpr, []*seq.QPR{&tmp}, math.MaxInt, 1, info.Request.Params.Order)
	}

	return FetchSearchResultResponse{
		QPR:          qpr,
		Expiration:   info.Expiration,
		Done:         info.Done,
		AggQueries:   info.Request.Params.AggQ,
		HistInterval: info.Request.Params.HistInterval,
		Order:        info.Request.Params.Order,
	}, true
}
