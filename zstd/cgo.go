//go:build cgo

// This code has been copied and modified from:
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/7551d799f740b2b255bd0b470ffc42dd6978a6b7/lib/encoding/zstd/zstd_cgo.go

package zstd

import (
	"github.com/valyala/gozstd"
)

// Decompress appends decompressed src to dst and returns the result.
func Decompress(src, dst []byte) ([]byte, error) {
	return gozstd.Decompress(dst, src)
}

// CompressLevel appends compressed src to dst and returns the result.
//
// The given compressionLevel is used for the compression.
func CompressLevel(src, dst []byte, compressionLevel int) []byte {
	return gozstd.CompressLevel(dst, src, compressionLevel)
}
