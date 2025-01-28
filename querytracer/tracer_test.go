package querytracer

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTracerDisabled(t *testing.T) {
	tr := New(false, "test")
	if tr.Enabled() {
		t.Fatalf("query tracer must be disabled")
	}

	trChild := tr.NewChild("child done %d", 456)
	if trChild.Enabled() {
		t.Fatalf("query tracer must be disabled")
	}

	trChild.Printf("foo %d", 123)
	trChild.Done()
	tr.Printf("parent %d", 789)
	tr.Done()

	require.Nil(t, tr.ToSpan())
}

func TestTracerEnabled(t *testing.T) {
	tr := New(true, "test")
	if !tr.Enabled() {
		t.Fatalf("query tracer must be enabled")
	}

	trChild := tr.NewChild("child done %d", 456)
	if !trChild.Enabled() {
		t.Fatalf("child query tracer must be enabled")
	}

	trChild.Printf("foo %d", 123)
	trChild.Done()
	tr.Printf("parent %d", 789)
	tr.Donef("foo %d", 33)

	expected := &Span{
		Message: "test: foo 33",
		Children: []*Span{
			{
				Message:  "child done 456",
				Children: []*Span{{Message: "foo 123"}},
			},
			{Message: "parent 789"},
		},
	}
	require.Equal(t, expected, zeroDurationsInSpan(tr.ToSpan()))
}

func TestTraceMissingDonef(t *testing.T) {
	tr := New(true, "parent")
	tr.Printf("parent printf")
	trChild := tr.NewChild("child")
	trChild.Printf("child printf")
	tr.Printf("another parent printf")

	expected := &Span{Message: "missing Tracer.Done() call for the trace with message=parent"}
	require.Equal(t, expected, zeroDurationsInSpan(tr.ToSpan()))
}

func TestTraceConcurrent(t *testing.T) {
	tr := New(true, "parent")
	childLocal := tr.NewChild("local")
	childLocal.Printf("abc")
	childLocal.Done()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		child := tr.NewChild("child %d", i)
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				child.Printf("message %d", j)
			}
			wg.Done()
		}()
	}

	tr.Done()
	// Verify that it is safe to call tr.ToSpan() when child traces aren't done yet
	s := tr.ToSpan()
	wg.Wait()

	expected := &Span{
		Message: "parent",
		Children: []*Span{
			{Message: "local", Children: []*Span{{Message: "abc"}}},
			{Message: "missing Tracer.Done() call for the trace with message=child 0"},
			{Message: "missing Tracer.Done() call for the trace with message=child 1"},
			{Message: "missing Tracer.Done() call for the trace with message=child 2"},
		},
	}
	require.Equal(t, expected, zeroDurationsInSpan(s))
}

func TestAddChildWithSpan(t *testing.T) {
	tr := New(true, "test")
	tr.Printf("printf")
	trChild := tr.NewChild("child")
	trChild.AddChildWithSpan(&Span{Message: "child span"})
	trChild.Done()
	tr.Done()

	expected := &Span{
		Message: "test",
		Children: []*Span{
			{Message: "printf"},
			{Message: "child", Children: []*Span{{Message: "child span"}}},
		},
	}
	require.Equal(t, expected, zeroDurationsInSpan(tr.ToSpan()))
}

func zeroDurationsInSpan(s *Span) *Span {
	s.Duration = 0
	for _, c := range s.Children {
		zeroDurationsInSpan(c)
	}
	return s
}
