package fracmanager

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/parser"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
	"go.uber.org/zap"
)

const (
	asyncSearchExtInfo      = ".info"
	asyncSearchExtQPR       = ".qpr"
	asyncSearchExtMergedQPR = ".mqpr"
	asyncSearchTmpFile      = ".tmp"
)

var (
	activeSearches = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "in_progress",
		Help:      "Amount of active async searches in progress",
	})
	diskUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "disk_usage_bytes",
	})
	storedRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "seq_db_store",
		Subsystem: "async_search",
		Name:      "stored_requests",
	})
)

type MappingProvider interface {
	GetMapping() seq.Mapping
}

type AsyncSearcher struct {
	config        AsyncSearcherConfig
	createDirOnce *sync.Once

	mp MappingProvider

	requestsMu *sync.RWMutex
	requests   map[string]asyncSearchInfo
	rateLimit  chan struct{}

	processWg *sync.WaitGroup
}

type AsyncSearcherConfig struct {
	DataDir     string
	Parallelism int
}

func MustStartAsync(config AsyncSearcherConfig, mp MappingProvider, fracs List) *AsyncSearcher {
	if config.DataDir == "" {
		logger.Fatal("can't start async searcher: DataDir is empty")
	}

	mustRemoveTmpFiles(config.DataDir)

	asyncSearches, err := loadAsyncSearches(config.DataDir)
	if err != nil {
		logger.Fatal("failed to load previous async searches", zap.Error(err))
	}

	parallelism := config.Parallelism
	if parallelism <= 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}

	as := &AsyncSearcher{
		config:        config,
		mp:            mp,
		requestsMu:    &sync.RWMutex{},
		requests:      asyncSearches,
		rateLimit:     make(chan struct{}, parallelism), // todo rename parallelism
		createDirOnce: &sync.Once{},
		processWg:     &sync.WaitGroup{},
	}

	notProcessedIDs := notProcessedTasks(asyncSearches)
	for _, id := range notProcessedIDs {
		activeSearches.Add(1)
		as.processWg.Add(1)
		go as.processRequest(id, fracs)
	}

	go as.startMaintenance()

	return as
}

type AsyncSearchRequest struct {
	ID        string
	Params    processor.SearchParams
	Query     string
	Retention time.Duration
}

type fracSearchState struct {
	Name string
}

type asyncSearchInfo struct {
	Done       bool
	Error      string `json:",omitempty"`
	Request    AsyncSearchRequest
	Fractions  []fracSearchState `json:",omitempty"`
	Expiration time.Time
	StartTime  time.Time
	CanceledAt time.Time `json:",omitempty"`
}

func (i *asyncSearchInfo) Canceled() bool {
	return !i.CanceledAt.IsZero()
}

func (i *asyncSearchInfo) Expired() bool {
	return i.Expiration.Before(time.Now())
}

func (as *AsyncSearcher) StartSearch(r AsyncSearchRequest, fracs List) error {
	as.requestsMu.RLock()
	_, ok := as.requests[r.ID]
	as.requestsMu.RUnlock()
	if ok {
		logger.Warn("async search already started", zap.String("id", r.ID))
		return nil
	}
	if _, err := uuid.Parse(r.ID); err != nil {
		return fmt.Errorf("invalid id %q: %s", r.ID, err)
	}

	ast, err := parser.ParseSeqQL(r.Query, as.mp.GetMapping())
	if err != nil {
		return err
	}
	r.Params.AST = ast.Root

	fracsToSearch := make([]fracSearchState, 0, len(fracs))
	for _, f := range fracs {
		fracsToSearch = append(fracsToSearch, fracSearchState{Name: f.Info().Name()})
	}

	// It can be empty if the replica does not contain the required data.
	// In this case, it is necessary to consider that the asynchronous request is completed.
	requestDone := len(fracs) == 0

	now := timeNow()
	if r.Retention < time.Minute*5 {
		return fmt.Errorf("retention time should be at least 5 minutes, got %s", r.Retention)
	}
	if now.Add(r.Retention).After(now.AddDate(5, 0, 0)) {
		// Just check Retention is correct. Retention more than 5 years is not expected.
		// More fine-grained validation by specific user/tenant/environment shouldn't be implemented in seq-db.
		return fmt.Errorf("retention time should be less than 5 years, got %s", r.Retention)
	}
	as.updateSearchInfo(r.ID, func(info *asyncSearchInfo) {
		if !info.StartTime.IsZero() {
			// Request has been started, it is concurrent update, do not
			return
		}
		*info = asyncSearchInfo{
			Done:       requestDone,
			Request:    r,
			Fractions:  fracsToSearch,
			Expiration: now.Add(r.Retention),
			StartTime:  now,
			CanceledAt: time.Time{},
		}
	})

	if !requestDone {
		activeSearches.Add(1)
		as.processWg.Add(1)
		go as.processRequest(r.ID, fracs)
	}

	return nil
}

func (as *AsyncSearcher) updateSearchInfo(id string, update func(info *asyncSearchInfo)) {
	as.requestsMu.Lock()
	defer as.requestsMu.Unlock()
	info := as.requests[id]
	update(&info)
	as.updateSearchInfoLocked(id, info)
}

func (as *AsyncSearcher) getSearchInfo(id string) (asyncSearchInfo, bool) {
	as.requestsMu.RLock()
	defer as.requestsMu.RUnlock()
	v, ok := as.requests[id]
	return v, ok
}

func (as *AsyncSearcher) updateSearchInfoLocked(id string, info asyncSearchInfo) {
	as.mustWriteSearchInfo(id, info)
	as.requests[id] = info
}

func (as *AsyncSearcher) mustWriteSearchInfo(id string, info asyncSearchInfo) {
	as.createDataDir()

	b := bytespool.Acquire(512)
	b.Reset()
	defer bytespool.Release(b)
	enc := json.NewEncoder(b)
	enc.SetIndent("", "\t")
	enc.SetEscapeHTML(false)

	if err := enc.Encode(info); err != nil {
		logger.Fatal("can't encode async request", zap.Error(err))
	}

	fpath := path.Join(as.config.DataDir, id+asyncSearchExtInfo)
	mustWriteFileAtomic(fpath, b.B)
}

// createDataDir creates dir data lazily to avoid creating extra folders.
func (as *AsyncSearcher) createDataDir() {
	as.createDirOnce.Do(func() {
		if err := os.MkdirAll(as.config.DataDir, 0o777); err != nil {
			panic(err)
		}
	})
}

func (as *AsyncSearcher) processRequest(asyncSearchID string, fracs List) {
	as.rateLimit <- struct{}{}
	defer func() { <-as.rateLimit }()

	as.doSearch(asyncSearchID, fracs)
	activeSearches.Add(-1)
	as.processWg.Done()
}

func (as *AsyncSearcher) doSearch(id string, fracs List) {
	qprPaths, err := as.findQPRs(id)
	if err != nil {
		panic(fmt.Errorf("can't find QPRs for id %q: %s", id, err))
	}

	processedFracs := make(map[string]struct{})
	for _, qprPath := range qprPaths {
		fracName, err := fracNameFromQPRPath(qprPath)
		if err != nil {
			logger.Fatal("cannot find previous QPRs", zap.Error(err))
		}
		processedFracs[fracName] = struct{}{}
	}

	info, ok := as.getSearchInfo(id)
	if !ok {
		panic(fmt.Errorf("BUG: can't find async search request for id %s", id))
	}

	logger.Info("starting async search request",
		zap.String("id", id),
		zap.Any("query", info.Request.Query),
		zap.Duration("interval", time.Duration(info.Request.Params.To-info.Request.Params.From)*time.Millisecond),
	)

	// AST can be nil in case of restarts.
	if info.Request.Params.AST == nil {
		ast, err := parser.ParseSeqQL(info.Request.Query, as.mp.GetMapping())
		if err != nil {
			panic(fmt.Errorf("BUG: search query must be valid: %s", err))
		}
		info.Request.Params.AST = ast.Root
	}

	fracsByName := make(map[string]frac.Fraction)
	for _, f := range fracs {
		fracsByName[f.Info().Name()] = f
	}

	for _, fracInfo := range info.Fractions {
		if _, ok := processedFracs[fracInfo.Name]; ok {
			continue
		}

		info, ok := as.getSearchInfo(id)
		if !ok {
			panic(fmt.Errorf("can't find async search request for id %s", id))
		}
		stop := info.Canceled() || info.Expired()
		if stop {
			break
		}

		f := fracsByName[fracInfo.Name]
		if err := as.processFrac(f, info.Request); err != nil {
			as.updateSearchInfo(id, func(info *asyncSearchInfo) {
				info.Error = err.Error()
			})
			break
		}
	}

	as.updateSearchInfo(id, func(info *asyncSearchInfo) {
		info.Done = true
	})
}

var qprMarshalBufPool util.BufferPool

func compressQPR(qpr *seq.QPR, cb func(compressed []byte)) {
	rawQPR := qprMarshalBufPool.Get()
	rawQPR.B = marshalQPR(qpr, rawQPR.B)

	compressionLevel := 3
	if len(rawQPR.B) <= 512 {
		compressionLevel = 1
	} else if len(rawQPR.B) <= 4*1024 {
		compressionLevel = 2
	}

	compressed := bytespool.AcquireReset(len(rawQPR.B))
	compressed.B = zstd.CompressLevel(rawQPR.B, compressed.B, compressionLevel)
	qprMarshalBufPool.Put(rawQPR)
	cb(compressed.B)
	bytespool.Release(compressed)
}

func (as *AsyncSearcher) processFrac(f frac.Fraction, r AsyncSearchRequest) error {
	dp, release := f.DataProvider(context.Background())
	qpr, err := dp.Search(r.Params)
	release()
	if err != nil {
		return err
	}

	fpath := path.Join(as.config.DataDir, r.ID+"."+f.Info().Name()+asyncSearchExtQPR) // <data-dir>/<request_id>.<frac_name>.qpr
	compressQPR(qpr, func(compressed []byte) {
		mustWriteFileAtomic(fpath, compressed)
	})

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

func (as *AsyncSearcher) findQPRs(id string) ([]string, error) {
	pattern := path.Join(as.config.DataDir, id+"*"+asyncSearchExtQPR)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func loadAsyncSearches(dataDir string) (map[string]asyncSearchInfo, error) {
	requests := make(map[string]asyncSearchInfo)

	des, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		filename := de.Name()
		if path.Ext(filename) != asyncSearchExtInfo {
			continue
		}

		parts := strings.Split(filename, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid .info filename format: %s", filename)
		}
		requestID := parts[0]
		b, err := os.ReadFile(path.Join(dataDir, filename))
		if err != nil {
			return nil, err
		}
		var req asyncSearchInfo
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, fmt.Errorf("malformed async search info %q: %s", filename, err)
		}
		// It is difficult to marshal/unmarshal AST, so set it to nil and parse it later.
		req.Request.Params.AST = nil
		requests[requestID] = req
	}

	return requests, nil
}

func notProcessedTasks(tasks map[string]asyncSearchInfo) []string {
	var toProcess []string
	for id := range tasks {
		task := tasks[id]
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

	Status  AsyncSearchStatus
	Done    int
	InQueue int

	// Stuff that needed seq-db proxy to complete async search response.
	AggQueries   []processor.AggQuery
	HistInterval uint64
	Order        seq.DocsOrder
}

type AsyncSearchStatus byte

const (
	AsyncSearchStatusDone AsyncSearchStatus = iota + 1
	AsyncSearchStatusError
	AsyncSearchStatusCanceled
	AsyncSearchStatusInProgress
)

func (as *AsyncSearcher) FetchSearchResult(r FetchSearchResultRequest) (FetchSearchResultResponse, bool) {
	info, ok := as.getSearchInfo(r.ID)
	if !ok || info.Canceled() {
		return FetchSearchResultResponse{}, false
	}

	var qprsPaths []string
	mergedFpath, merged := as.qprsMerged(r.ID)
	if merged {
		qprsPaths = []string{mergedFpath}
	} else {
		// todo do not conflict with the merge stage
		v, err := as.findQPRs(r.ID)
		if err != nil {
			logger.Fatal("can't load async search result", zap.String("id", r.ID), zap.Error(err))
		}
		qprsPaths = v
	}
	qpr := as.loadSearchResult(qprsPaths, info.Request.Params.Order)

	var status AsyncSearchStatus
	var fracsDone, fracsInQueue int
	if info.Canceled() {
		status = AsyncSearchStatusCanceled
		fracsDone = len(qprsPaths)
		fracsInQueue = len(info.Fractions)
	} else if info.Error != "" {
		status = AsyncSearchStatusError
		fracsDone = len(qprsPaths)
		fracsInQueue = 0
	} else if info.Done {
		status = AsyncSearchStatusDone
		fracsDone = len(info.Fractions)
		fracsInQueue = 0
	} else {
		status = AsyncSearchStatusInProgress
		fracsDone = len(qprsPaths)
		fracsInQueue = len(info.Fractions)
	}

	return FetchSearchResultResponse{
		QPR:          qpr,
		Expiration:   info.Expiration,
		Status:       status,
		Done:         fracsDone,
		InQueue:      fracsInQueue,
		AggQueries:   info.Request.Params.AggQ,
		HistInterval: info.Request.Params.HistInterval,
		Order:        info.Request.Params.Order,
	}, true
}

func (as *AsyncSearcher) loadSearchResult(qprsPaths []string, order seq.DocsOrder) seq.QPR {
	qpr := seq.QPR{}
	for _, qprPath := range qprsPaths {
		compressedQPR, err := os.ReadFile(qprPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return seq.QPR{}
			}
			logger.Fatal("can't read async search result from file", zap.String("path", qprPath), zap.Error(err))
		}
		qprRaw, err := zstd.Decompress(compressedQPR, nil)
		if err != nil {
			logger.Fatal("can't decompress async search result", zap.String("path", qprPath), zap.Error(err))
		}

		const idsLimit = math.MaxInt
		var tmp seq.QPR
		tail, err := unmarshalQPR(&tmp, qprRaw, idsLimit, 0)
		if err != nil {
			logger.Fatal("can't unmarshal async search result", zap.String("path", qprPath), zap.Error(err))
		}
		if len(tail) > 0 {
			logger.Fatal("unexpected tail when unmarshalling binary QPR", zap.String("path", qprPath))
		}
		seq.MergeQPRs(&qpr, []*seq.QPR{&tmp}, idsLimit, 1, order)
	}
	return qpr
}

var timeNow = time.Now

func (as *AsyncSearcher) startMaintenance() {
	for {
		now := timeNow()
		as.removeExpiredResults(now)
		as.mergeQPRs()
		as.updateStats()
		const maintenanceInterval = 5 * time.Second
		time.Sleep(maintenanceInterval)
	}
}

func (as *AsyncSearcher) mergeQPRs() {
	now := timeNow()
	var toMerge []string
	as.requestsMu.RLock()
	for id := range as.requests {
		r := as.requests[id]
		if r.Canceled() || !r.Done {
			continue
		}
		if len(r.Fractions) < 2 {
			// Nothing to merge
			continue
		}
		if r.Expiration.Sub(now) < time.Hour {
			// Do not merge QPRs that will be expired soon
			continue
		}
		toMerge = append(toMerge, id)
	}
	as.requestsMu.RUnlock()

	for _, id := range toMerge {
		as.mergeQPR(id)
	}
}

func (as *AsyncSearcher) mergeQPR(id string) {
	mergedFilename, alreadyMerged := as.qprsMerged(id)

	qprPaths, err := as.findQPRs(id)
	if err != nil {
		logger.Fatal("can't load async search results", zap.String("id", id), zap.Error(err))
	}
	if !alreadyMerged {
		// Order isn't matter here, since it will be restored in Fetch
		qpr := as.loadSearchResult(qprPaths, seq.DocsOrderAsc)
		compressQPR(&qpr, func(compressed []byte) {
			mustWriteFileAtomic(mergedFilename, compressed)
		})
	}
	for _, qprPath := range qprPaths {
		// Remove unnecessary QPRs since we have merged QPR result
		if err := os.Remove(qprPath); err != nil {
			logger.Fatal("can't remove async search result", zap.String("id", id), zap.Error(err))
		}
	}
}

func (as *AsyncSearcher) qprsMerged(id string) (string, bool) {
	mergedFilename := id + asyncSearchExtMergedQPR
	fpath := path.Join(as.config.DataDir, mergedFilename)
	alreadyMerged := fileExists(fpath)
	return fpath, alreadyMerged
}

func (as *AsyncSearcher) removeExpiredResults(now time.Time) {
	var toRemove []string
	as.requestsMu.RLock()
	for id := range as.requests {
		r := as.requests[id]
		if r.Expiration.After(now) {
			continue
		}
		if r.Done {
			// Async search request is done and expired
			toRemove = append(toRemove, id)
			continue
		}
		if !r.Canceled() {
			// Async search request isn't done, so cancel it
			as.requestsMu.RUnlock()
			as.updateSearchInfo(id, func(info *asyncSearchInfo) {
				info.CanceledAt = now
			})
			as.requestsMu.RLock()
		}
	}
	as.requestsMu.RUnlock()

	as.requestsMu.Lock()
	// Exclude expired requests from search result
	for _, id := range toRemove {
		delete(as.requests, id)
	}
	as.requestsMu.Unlock()

	for _, id := range toRemove {
		qprPaths, err := as.findQPRs(id)
		if err != nil {
			logger.Fatal("can't load async search results", zap.String("id", id), zap.Error(err))
		}
		for _, qprPath := range qprPaths {
			removeFile(qprPath)
		}
		removeFile(path.Join(as.config.DataDir, id+asyncSearchExtMergedQPR))
		removeFile(path.Join(as.config.DataDir, id+asyncSearchExtInfo))
	}
}

func (as *AsyncSearcher) updateStats() {
	files, err := os.ReadDir(as.config.DataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		logger.Fatal("can't read dir", zap.String("dir", as.config.DataDir), zap.Error(err))
	}

	diskUsageBytes := int64(0)
	infos := 0
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		info, err := f.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			logger.Error("can't get async search file info", zap.String("dir", as.config.DataDir), zap.Error(err))
			continue
		}
		ext := path.Ext(info.Name())
		if ext == asyncSearchExtInfo {
			infos++
		}
		diskUsageBytes += info.Size()
	}
	diskUsage.Set(float64(diskUsageBytes))
	storedRequests.Set(float64(infos))
}

func mustWriteFileAtomic(fpath string, data []byte) {
	fpathTmp := fpath + asyncSearchTmpFile

	f, err := os.Create(fpathTmp)
	if err != nil {
		logger.Fatal("can't create file", zap.Error(err))
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Fatal("can't close file", zap.Error(err))
		}
	}()

	if _, err := f.Write(data); err != nil {
		logger.Fatal("can't write to file", zap.Error(err))
	}

	if err := f.Sync(); err != nil {
		logger.Fatal("can't sync file", zap.Error(err))
	}

	if err := os.Rename(fpathTmp, fpath); err != nil {
		logger.Fatal("can't rename file", zap.Error(err))
	}

	absFpath, err := filepath.Abs(fpath)
	if err != nil {
		logger.Fatal("can't get absolute path", zap.String("path", fpath), zap.Error(err))
	}
	dir := path.Dir(absFpath)
	mustFsyncFile(dir)
}

func mustRemoveTmpFiles(dir string) {
	des, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		logger.Fatal("can't read directory with async searches", zap.Error(err))
	}
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		n := de.Name()
		if path.Ext(n) != asyncSearchTmpFile {
			continue
		}
		p := filepath.Join(dir, n)
		if err := os.Remove(p); err != nil {
			logger.Fatal("can't remove tmp file", zap.String("file", n), zap.Error(err))
		}
	}
	mustFsyncFile(dir)
}

func mustFsyncFile(fpath string) {
	dirFile, err := os.Open(fpath)
	if err != nil {
		logger.Fatal("can't open dir", zap.Error(err))
	}
	if err := dirFile.Sync(); err != nil {
		logger.Fatal("can't sync dir", zap.Error(err))
	}
	if err := dirFile.Close(); err != nil {
		logger.Fatal("can't close dir", zap.Error(err))
	}
}

func fileExists(fpath string) bool {
	_, err := os.Stat(fpath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		logger.Fatal("can't stat file", zap.Error(err))
	}
	return true
}
