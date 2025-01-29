package mappingprovider

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/seq"
)

const initialMappingData = `mapping-list:
  - name: "k8s_pod"
    type: "keyword"
`

const changedMappingData = `mapping-list:
  - name: "k8s_pod"
    type: "keyword"
  - name: "trace_id"
    type: "keyword"
`

func TestMappingProvider(t *testing.T) {
	dir := t.TempDir()
	mappingFilePath := filepath.Join(dir, "mappings.yaml")

	err := os.WriteFile(mappingFilePath, []byte(initialMappingData), 0o644)
	require.NoError(t, err)

	mappingProvider, err := New(mappingFilePath, WithUpdatePeriod(100*time.Millisecond))
	require.NoError(t, err)

	assert.Equal(t, seq.Mapping{"k8s_pod": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0)}, mappingProvider.GetMapping())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mappingProvider.WatchUpdates(ctx)

	err = os.WriteFile(mappingFilePath, []byte(changedMappingData), 0o644)
	require.NoError(t, err)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, seq.Mapping{
			"k8s_pod":  seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
			"trace_id": seq.NewSingleType(seq.TokenizerTypeKeyword, "", 0),
		}, mappingProvider.GetMapping())
	}, 5*time.Second, 500*time.Millisecond)
}
