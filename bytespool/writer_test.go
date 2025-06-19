package bytespool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	r := require.New(t)
	output := bytes.NewBuffer(nil)
	w := AcquireWriterSize(output, 0)

	test := func(w *Writer, payload, expectedOutput, expectedWriterBuf string) {
		t.Helper()
		n, err := w.WriteString(payload)
		r.NoError(err)
		r.Equal(len(payload), n)
		r.Equal(expectedOutput, output.String())
		r.Equal(expectedWriterBuf, string(w.Buf.B))
	}

	// Zero size and empty data
	output.Reset()
	w.Buf.B = make([]byte, 0)
	test(w, "", "", "")
	test(w, "", "", "")
	r.NoError(w.Flush())
	r.Equal("", string(w.Buf.B))
	r.Equal("", output.String())
	test(w, "", "", "")
	r.Equal("", string(w.Buf.B))
	r.Equal("", output.String())

	// Empty data
	output.Reset()
	w.Buf.B = make([]byte, 0, 4)
	test(w, "", "", "")
	test(w, "", "", "")
	test(w, "", "", "")
	r.NoError(w.Flush())
	r.Equal("", string(w.Buf.B))
	r.Equal("", output.String())

	// Tiny buffer
	output.Reset()
	w.Buf.B = make([]byte, 0, 4)
	payload := "more than 4"
	test(w, payload, payload, "")
	r.NoError(w.Flush())
	r.Equal("", string(w.Buf.B))
	r.Equal(payload, output.String())

	// Large buffer
	output.Reset()
	w.Buf.B = make([]byte, 0, 1<<10)
	payload = "more than 4"
	test(w, payload, "", payload)
	r.NoError(w.Flush())
	r.Equal("", string(w.Buf.B))
	r.Equal(payload, output.String())

	// Flush buffer state
	output.Reset()
	w.Buf.B = make([]byte, 0, 4)
	payload = "123"
	test(w, payload, "", payload)
	payload2 := "_this message must flush previous state"
	test(w, payload2, payload+payload2, "")
	r.Equal("", string(w.Buf.B))
	r.Equal(payload+payload2, output.String())
	r.NoError(w.Flush())
	r.Equal("", string(w.Buf.B))
	r.Equal(payload+payload2, output.String())

	// Flush buffer state 2
	output.Reset()
	w.Buf.B = make([]byte, 0, 4)
	test(w, "1", "", "1")
	test(w, "23", "", "123")
	test(w, "456", "1234", "56")
	r.NoError(w.Flush())
	r.Equal("", string(w.Buf.B))
	r.Equal("123456", output.String())
}
