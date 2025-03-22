package frac

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type SealParams struct {
	IDsZstdLevel           int
	LIDsZstdLevel          int
	TokenListZstdLevel     int
	DocsPositionsZstdLevel int
	TokenTableZstdLevel    int

	// DocBlocksZstdLevel is the zstd compress level of each document block.
	DocBlocksZstdLevel int
	// DocBlockSize is decompressed payload size of document block.
	DocBlockSize int
}

func seal(f *Active, params SealParams) {
	logger.Info("sealing fraction", zap.String("fraction", f.BaseFileName))

	start := time.Now()
	info := f.Info()
	if info.To == 0 {
		logger.Panic("sealing of an empty active fraction is not supported")
	}

	f.setInfoSealingTime(uint64(time.Now().UnixMilli()))

	tmpIndexFileName := f.BaseFileName + consts.IndexTmpFileSuffix
	indexFile, err := os.OpenFile(tmpIndexFileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o776)
	if err != nil {
		logger.Fatal("can't open file", zap.String("file", tmpIndexFileName), zap.Error(err))
	}

	_, err = indexFile.Seek(16, io.SeekStart) // skip 16 bytes for pos and length of registry
	if err != nil {
		logger.Fatal("can't seek file", zap.String("file", indexFile.Name()), zap.Error(err))
	}

	tmpSdocsFileName := f.BaseFileName + consts.SortedDocsTmpFileSuffix
	sdocsFile, err := os.OpenFile(tmpSdocsFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o776)
	if err != nil {
		logger.Fatal("can't open file", zap.String("file", tmpSdocsFileName), zap.Error(err))
	}

	if err = writeSealedFraction(f, indexFile, sdocsFile, params); err != nil {
		logger.Fatal("can't write sealed fraction", zap.String("fraction", f.BaseFileName), zap.Error(err))
	}

	sdocsFileName := f.BaseFileName + consts.SortedDocsFileSuffix
	if err := os.Rename(tmpSdocsFileName, sdocsFileName); err != nil {
		logger.Fatal("can't rename sdocs file", zap.String("file", tmpSdocsFileName), zap.Error(err))
	}

	stat, err := indexFile.Stat() // refresh f.info.IndexOnDisk - it will be used later
	if err != nil {
		logger.Fatal("can't stat index file", zap.String("file", indexFile.Name()), zap.Error(err))
	}
	f.setInfoIndexOnDisk(uint64(stat.Size()))

	if err := indexFile.Sync(); err != nil {
		logger.Fatal("can't sync tmp index file", zap.String("file", indexFile.Name()), zap.Error(err))
	}

	if err = indexFile.Close(); err != nil {
		logger.Fatal("can't close file", zap.String("file", indexFile.Name()), zap.Error(err))
	}

	f.close(false, "seal")

	newFileName := f.BaseFileName + consts.IndexFileSuffix
	err = os.Rename(tmpIndexFileName, newFileName)
	if err != nil {
		logger.Error("can't rename index file",
			zap.String("old_path", tmpIndexFileName),
			zap.String("new_path", newFileName),
			zap.Error(err),
		)
	}

	logger.Info(
		"fraction sealed",
		zap.String("fraction", newFileName),
		zap.Float64("time_spent_s", util.DurationToUnit(time.Since(start), "s")),
	)
}

func writeSealedFraction(f *Active, indexFile, sdocsFile *os.File, params SealParams) error {
	var err error
	sortedIDs, oldToNewLIDsIndex := sortSeqIDs(f, f.MIDs.GetVals(), f.RIDs.GetVals())

	logger.Info("sorting docs...")
	bw := getDocBlocksWriter(sdocsFile, params.DocBlockSize, params.DocBlocksZstdLevel)
	defer putDocBlocksWriter(bw)
	if err := writeDocsInOrder(f, sortedIDs, bw); err != nil {
		return fmt.Errorf("writing sorted docs: %s", err)
	}
	if err := sdocsFile.Sync(); err != nil {
		return fmt.Errorf("syncing sorted docs file: %s", err)
	}
	f.sortedDocsFile = sdocsFile

	producer := NewDiskBlocksProducer()
	writer := NewSealedBlockWriter(indexFile)
	{
		logger.Info("sealing frac stats...")
		f.BuildInfoDistribution(sortedIDs)
		fracInfo := f.Info()
		if err := writer.writeInfoBlock(producer.getInfoBlock(fracInfo)); err != nil {
			logger.Error("seal info error", zap.Error(err))
			return err
		}
	}

	var tokenTable token.Table
	{
		logger.Info("sealing tokens...")
		generator := producer.getTokensBlocksGenerator(f.TokenList)
		tokenTable, err = writer.writeTokensBlocks(params.TokenListZstdLevel, generator)
		if err != nil {
			logger.Error("sealing tokens error", zap.Error(err))
			return err
		}
	}

	{
		logger.Info("sealing tokens table...")
		generator := producer.getTokenTableBlocksGenerator(f.TokenList, tokenTable)
		if err := writer.writeTokenTableBlocks(params.TokenTableZstdLevel, generator); err != nil {
			logger.Error("sealing tokens table error", zap.Error(err))
			return err
		}
	}

	{
		logger.Info("writing document positions block...")
		idsLen := f.MIDs.Len()
		generator := producer.getPositionBlock(idsLen, bw.BlockOffsets)
		if err := writer.writePositionsBlock(params.DocsPositionsZstdLevel, generator); err != nil {
			logger.Error("document positions block error", zap.Error(err))
			return err
		}
	}

	var minBlockIDs []seq.ID
	{
		logger.Info("sealing ids...")
		ds := DocsPositions{positions: bw.Positions}
		generator := producer.getIDsBlocksGenerator(sortedIDs, &ds, consts.IDsBlockSize)
		minBlockIDs, err = writer.writeIDsBlocks(params.IDsZstdLevel, generator)
		if err != nil {
			logger.Error("seal ids error", zap.Error(err))
			return err
		}
	}

	var lidsTable *lids.Table
	{
		logger.Info("sealing lids...")
		generator := producer.getLIDsBlockGenerator(f.TokenList, oldToNewLIDsIndex, f.MIDs, f.RIDs, consts.LIDBlockCap)
		lidsTable, err = writer.writeLIDsBlocks(params.LIDsZstdLevel, generator)
		if err != nil {
			logger.Error("seal lids error", zap.Error(err))
			return err
		}
	}

	logger.Info("write registry...")
	if err = writer.WriteRegistryBlock(); err != nil {
		logger.Error("write registry error", zap.Error(err))
		return err
	}

	// these fields actually aren't not used as intended: the data of these three fields will actually be read
	// from disk again in the future on the first attempt to search in fraction (see method Sealed.loadAndRLock())
	// TODO: we need to either remove this data preparation in active fraction sealing or avoid re-reading the data from disk
	f.sealedIDs = &SealedIDs{
		IDBlocksTotal:       0,
		DiskStartBlockIndex: writer.startOfIDsBlockIndex,
		MinBlockIDs:         minBlockIDs,
		IDsTotal:            uint32(len(f.RIDs.GetVals())),
	}
	f.lidsTable = lidsTable
	f.tokenTable = tokenTable

	writer.stats.WriteLogs()

	return nil
}

func writeDocsInOrder(f *Active, ids []seq.ID, bw *docBlocksWriter) error {
	// Skip system seq.ID.
	if len(ids) == 0 {
		panic(fmt.Errorf("BUG: ids is empty"))
	}
	if ids[0] != systemSeqID {
		panic(fmt.Errorf("BUG: system ID expected"))
	}
	ids = ids[1:]

	if err := writeDocBlocksInOrder(f, ids, bw); err != nil {
		return err
	}
	return nil
}

func writeDocBlocksInOrder(f *Active, ids []seq.ID, bw *docBlocksWriter) error {
	for _, id := range ids {
		oldPos := f.DocsPositions.Get(id)
		if oldPos == DocPosNotFound {
			panic(fmt.Errorf("BUG: can't find doc position"))
		}

		blockOffsetIndex, offset := oldPos.Unpack()
		blockOffset := f.DocBlocks.vals[blockOffsetIndex]
		docs, err := f.frac.readDocs(blockOffset, []uint64{offset})
		if err != nil {
			return err
		}
		doc := docs[0]
		if err := bw.WriteDoc(id, doc); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return nil
}

type docBlocksWriter struct {
	w             *bytespool.Writer
	compressLevel int
	minBlockSize  int

	curBlockIndex      int
	currentBlockOffset uint64

	docs     []byte
	blockBuf []byte

	BlockOffsets []uint64
	Positions    map[seq.ID]DocPos
}

var docBlocksWriterPool = sync.Pool{
	New: func() any {
		return &docBlocksWriter{Positions: make(map[seq.ID]DocPos)}
	},
}

func getDocBlocksWriter(w io.Writer, blockSize, compressLevel int) *docBlocksWriter {
	bw := docBlocksWriterPool.Get().(*docBlocksWriter)

	if blockSize <= 0 {
		blockSize = consts.MB * 4
	}

	bufSize := consts.MB * 32
	if bufSize < blockSize {
		bufSize = blockSize
	}

	*bw = docBlocksWriter{
		w:             bytespool.AcquireWriterSize(w, bufSize),
		compressLevel: compressLevel,
		minBlockSize:  blockSize,

		curBlockIndex:      0,
		currentBlockOffset: 0,

		docs:     bw.docs[:0],
		blockBuf: bw.blockBuf[:0],

		BlockOffsets: bw.BlockOffsets[:0],
		Positions:    bw.Positions,
	}
	clear(bw.Positions)

	return bw
}

func putDocBlocksWriter(bw *docBlocksWriter) {
	err := bytespool.FlushReleaseWriter(bw.w)
	if err != nil {
		panic(fmt.Errorf("BUG: writer must be flushed before releasing blocks writer: %s", err))
	}
	bw.w = nil
	docBlocksWriterPool.Put(bw)
}

func (w *docBlocksWriter) WriteDoc(id seq.ID, doc []byte) error {
	pos := PackDocPos(uint32(w.curBlockIndex), uint64(len(w.docs)))
	w.Positions[id] = pos

	w.docs = binary.LittleEndian.AppendUint32(w.docs, uint32(len(doc)))
	w.docs = append(w.docs, doc...)

	if len(w.docs) > w.minBlockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

func (w *docBlocksWriter) flushBlock() error {
	blockLen, err := w.compressWriteBlock()
	if err != nil {
		return err
	}

	w.docs = w.docs[:0]
	w.BlockOffsets = append(w.BlockOffsets, w.currentBlockOffset)
	w.curBlockIndex++
	w.currentBlockOffset += uint64(blockLen)

	return nil
}

func (w *docBlocksWriter) compressWriteBlock() (int, error) {
	w.blockBuf = w.blockBuf[:0]
	w.blockBuf = disk.CompressDocBlock(w.docs, w.blockBuf, w.compressLevel)

	if _, err := w.w.Write(w.blockBuf); err != nil {
		return 0, err
	}

	blockLen := len(w.blockBuf)
	return blockLen, nil
}

func (w *docBlocksWriter) Flush() error {
	if len(w.docs) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return nil
}
