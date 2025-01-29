package proxyapi

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/ozontech/seq-db/pkg/seqproxyapi/v1"
	"github.com/ozontech/seq-db/seq"
)

type mappingTestCaseData struct {
	mapping seq.Mapping
	noResp  bool
}

type mappingTestData struct {
	req   *seqproxyapi.MappingRequest
	want  *seqproxyapi.MappingResponse
	mData *mocksData
}

func prepareMappingTestData(cData mappingTestCaseData) mappingTestData {
	var resp *seqproxyapi.MappingResponse
	if !cData.noResp {
		resp = &seqproxyapi.MappingResponse{Data: seq.NewRawMapping(cData.mapping).GetRawMappingBytes()}
	}
	return mappingTestData{
		req:  &seqproxyapi.MappingRequest{},
		want: resp,
		mData: &mocksData{
			ac: &acMockData{
				mapping: cData.mapping,
			},
		},
	}
}

func TestGrpcV1_Mapping(t *testing.T) {
	tests := []struct {
		name    string
		data    mappingTestCaseData
		wantErr bool
	}{
		{
			name: "ok",
			data: mappingTestCaseData{
				mapping: seq.Mapping{"message": seq.NewSingleType(seq.TokenizerTypeText, "", 0)},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			testData := prepareMappingTestData(tt.data)
			a := prepareTestGrpcV1(ctrl, testData.mData)

			got, err := a.s.Mapping(a.ctx, testData.req)
			r.Equal(tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}

			r.NotNil(got)
			r.Equal(testData.want.Data, got.Data)
		})
	}
}
