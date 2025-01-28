package seqproxyapi

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAggregationBucketMarshalJSON(t *testing.T) {
	r := require.New(t)
	test := func(bucket *Aggregation_Bucket, expected string) {
		t.Helper()

		raw, err := json.Marshal(bucket)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &Aggregation_Bucket{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		// Handle math.NaN and math.Inf.
		if math.IsNaN(bucket.Value) || math.IsInf(bucket.Value, 0) {
			r.True(math.IsNaN(unmarshaled.Value) || math.IsInf(unmarshaled.Value, 0))
			bucket.Value = 0
			unmarshaled.Value = 0
		}
		r.Equal(bucket, unmarshaled)
	}

	test(&Aggregation_Bucket{}, `{"value":0}`)
	test(&Aggregation_Bucket{Value: 42}, `{"value":42}`)
	test(&Aggregation_Bucket{Value: math.NaN()}, `{"value":"NaN"}`)
	test(&Aggregation_Bucket{Value: math.Inf(1)}, `{"value":"+Inf"}`)
	test(&Aggregation_Bucket{Value: math.Inf(-1)}, `{"value":"-Inf"}`)
}

func TestStoreStatusValuesMarshalJSON(t *testing.T) {
	r := require.New(t)
	test := func(storeStatus *StoreStatusValues, expected string) {
		t.Helper()

		raw, err := json.Marshal(storeStatus)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &StoreStatusValues{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		r.Equal(storeStatus, unmarshaled)
	}

	test(&StoreStatusValues{OldestTime: timestamppb.New(time.UnixMilli(999))}, `{"oldest_time":"1970-01-01T00:00:00.999Z"}`)
	test(&StoreStatusValues{OldestTime: timestamppb.New(time.UnixMilli(9999999))}, `{"oldest_time":"1970-01-01T02:46:39.999Z"}`)
}

func TestStatusResponseMarshalJSON(t *testing.T) {
	r := require.New(t)
	test := func(status *StatusResponse, expected string) {
		t.Helper()

		raw, err := json.Marshal(status)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &StatusResponse{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		r.Equal(status, unmarshaled)
	}

	test(&StatusResponse{OldestStorageTime: nil}, `{"oldest_storage_time":null}`)
	test(&StatusResponse{OldestStorageTime: timestamppb.New(time.UnixMilli(999))}, `{"oldest_storage_time":"1970-01-01T00:00:00.999Z"}`)
	test(&StatusResponse{OldestStorageTime: timestamppb.New(time.UnixMilli(9999999))}, `{"oldest_storage_time":"1970-01-01T02:46:39.999Z"}`)
}

func TestExplainEntryMarshalJSON(t *testing.T) {
	r := require.New(t)

	test := func(explainEntry *ExplainEntry, expected string) {
		t.Helper()

		raw, err := json.Marshal(explainEntry)
		r.NoError(err)
		r.Equal(expected, string(raw))

		unmarshaled := &ExplainEntry{}
		r.NoError(json.Unmarshal(raw, unmarshaled))

		r.Equal(explainEntry, unmarshaled)
	}

	test(&ExplainEntry{Duration: durationpb.New(12 * time.Microsecond)}, `{"duration":"12µs"}`)
	test(&ExplainEntry{Duration: durationpb.New(8*time.Millisecond + 58*time.Microsecond)}, `{"duration":"8.058ms"}`)
	test(&ExplainEntry{Duration: durationpb.New(1*time.Second + 12*time.Millisecond)}, `{"duration":"1.012s"}`)
	test(&ExplainEntry{Duration: durationpb.New(2*time.Minute + 28*time.Second + 12*time.Millisecond)}, `{"duration":"2m28.012s"}`)
}
