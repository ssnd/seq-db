package conf

import (
	"runtime"

	"github.com/ozontech/seq-db/consts"
)

func init() {
	NumCPU := runtime.GOMAXPROCS(0)
	IndexWorkers = NumCPU
	FetchWorkers = NumCPU
	ReaderWorkers = NumCPU
}

var (
	NumCPU        int
	IndexWorkers  int
	FetchWorkers  int
	ReaderWorkers int

	CaseSensitive = false
	SkipFsync     = false

	MaxFetchSizeBytes = 4 * consts.MB

	MaxRequestedDocuments = 100_000 // maximum number of documents that can be requested in one fetch request

	UseSeqQLByDefault = false

	SortDocs = true
)
