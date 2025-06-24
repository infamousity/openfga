package commands

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
)

// Mock implementations.
type mockTupleReader struct {
	storage.RelationshipTupleReader
}
type mockCheckResolver struct{ graph.CheckResolver }

func TestNewShadowedListObjectsQuery(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		noopLogger := logger.NewNoopLogger()
		result, err := newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(
			WithShadowListObjectsQuerySamplePercentage(13),
		), WithListObjectsOptimizationEnabled(true))
		require.NoError(t, err)
		require.NotNil(t, result)
		query := result.(*shadowedListObjectsQuery)
		assert.False(t, query.standard.(*listObjectsQuery).listObjectsOptimizationEnabled)
		assert.True(t, query.optimized.(*listObjectsQuery).listObjectsOptimizationEnabled)
		assert.Equal(t, noopLogger, query.logger)
	})

	t.Run("ds_error", func(t *testing.T) {
		result, err := newShadowedListObjectsQuery(nil, &mockCheckResolver{}, NewShadowListObjectsQueryConfig())
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("check_resolver_error", func(t *testing.T) {
		result, err := newShadowedListObjectsQuery(&mockTupleReader{}, nil, NewShadowListObjectsQueryConfig())
		require.Error(t, err)
		require.Nil(t, result)
	})
}

type mockListObjectsQuery struct {
	executeFunc         func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error)
	executeStreamedFunc func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error)
}

func (m *mockListObjectsQuery) Execute(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
	return m.executeFunc(ctx, req)
}
func (m *mockListObjectsQuery) ExecuteStreamed(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
	return m.executeStreamedFunc(ctx, req, srv)
}

func TestShadowedListObjectsQuery_Execute(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &openfgav1.ListObjectsRequest{}
	expected := &ListObjectsResponse{Objects: []string{"foo"}}
	expectedOpt := &ListObjectsResponse{Objects: []string{"foo"}}

	tests := []struct {
		name            string
		percentage      int
		standardErr     error
		optimizedErr    error
		standardResult  *ListObjectsResponse
		optimizedResult *ListObjectsResponse
		expectErr       bool
		expectResult    *ListObjectsResponse
	}{
		{
			name:            "both_succeed_with_equal_results",
			standardResult:  expected,
			optimizedResult: expectedOpt,
			expectResult:    expected,
			percentage:      100,
		},
		{
			name:            "standard_fails",
			standardErr:     errors.New("fail"),
			optimizedResult: expectedOpt,
			expectErr:       true,
			percentage:      100,
		},
		{
			name:           "optimized_fails",
			standardResult: expected,
			optimizedErr:   errors.New("fail"),
			expectResult:   expected,
			percentage:     100,
		},
		{
			name:           "turned_off_shadow_mode",
			standardResult: expected,
			optimizedErr:   errors.New("ignored"),
			expectResult:   expected,
			percentage:     0, // never run shadow
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			q := &shadowedListObjectsQuery{
				standard: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return tt.standardResult, tt.standardErr
					},
				},
				optimized: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						return tt.optimizedResult, tt.optimizedErr
					},
				},
				logger:     logger.NewNoopLogger(),
				percentage: tt.percentage,
				random:     rand.New(rand.NewSource(time.Now().UnixNano())),
			}
			result, err := q.Execute(ctx, req)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResult, result)
			}
		})
	}
}

func TestShadowedListObjectsQuery_Panics(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &openfgav1.ListObjectsRequest{}

	tests := []struct {
		name       string
		percentage int
	}{
		{
			name:       "both_panics",
			percentage: 100,
		},
		{
			name:       "stardard_panics",
			percentage: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			q := &shadowedListObjectsQuery{
				standard: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						panic("this is a panic in standard query")
					},
				},
				optimized: &mockListObjectsQuery{
					executeFunc: func(ctx context.Context, req *openfgav1.ListObjectsRequest) (*ListObjectsResponse, error) {
						panic("this is a panic in optimized query")
					},
				},
				logger:     logger.NewNoopLogger(),
				percentage: tt.percentage,
				random:     rand.New(rand.NewSource(time.Now().UnixNano())),
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						// should only panic in standard mode
						assert.Equal(t, 0, tt.percentage)
					}
				}()
				_, err := q.Execute(ctx, req)
				// in shadow mode, panics are handled and returned as errors
				require.Error(t, err)
				assert.Equal(t, 100, tt.percentage)
			}()
		})
	}
}

func TestShadowedListObjectsQuery_ExecuteStreamed(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	req := &openfgav1.StreamedListObjectsRequest{}
	expected := &ListObjectsResolutionMetadata{}
	expectedOpt := &ListObjectsResolutionMetadata{}

	tests := []struct {
		name            string
		standardErr     error
		optimizedErr    error
		standardResult  *ListObjectsResolutionMetadata
		optimizedResult *ListObjectsResolutionMetadata
		expectErr       bool
		expectResult    *ListObjectsResolutionMetadata
	}{
		{
			name:            "both_succeed_with_equal_results",
			standardResult:  expected,
			optimizedResult: expectedOpt,
			expectResult:    expected,
		},
		{
			name:            "standard_fails",
			standardErr:     errors.New("fail"),
			optimizedResult: expectedOpt,
			expectErr:       true,
		},
		{
			name:           "optimized_fails",
			standardResult: expected,
			optimizedErr:   errors.New("fail"),
			expectResult:   expected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			q := &shadowedListObjectsQuery{
				standard: &mockListObjectsQuery{
					executeStreamedFunc: func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
						return tt.standardResult, tt.standardErr
					},
				},
				optimized: &mockListObjectsQuery{
					executeStreamedFunc: func(ctx context.Context, req *openfgav1.StreamedListObjectsRequest, srv openfgav1.OpenFGAService_StreamedListObjectsServer) (*ListObjectsResolutionMetadata, error) {
						return tt.optimizedResult, tt.optimizedErr
					},
				},
				logger:     logger.NewNoopLogger(),
				percentage: 100, // Always run in shadow mode for testing
				random:     rand.New(rand.NewSource(time.Now().UnixNano())),
			}
			result, err := q.ExecuteStreamed(ctx, req, nil)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectResult, result)
			}
		})
	}
}

func TestShadowedListObjectsQuery_isShadowModeEnabled(t *testing.T) {
	q, _ := newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(WithShadowListObjectsQueryEnabled(true), WithShadowListObjectsQuerySamplePercentage(100)))
	sq, ok := q.(*shadowedListObjectsQuery)
	require.True(t, ok)
	assert.True(t, sq.checkShadowModeSampleRate())

	q, _ = newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, NewShadowListObjectsQueryConfig(WithShadowListObjectsQueryEnabled(true), WithShadowListObjectsQuerySamplePercentage(0)))
	sq, ok = q.(*shadowedListObjectsQuery)
	require.True(t, ok)
	assert.False(t, sq.checkShadowModeSampleRate())
}

func TestShadowedListObjectsQuery_nilConfig(t *testing.T) {
	_, err := newShadowedListObjectsQuery(&mockTupleReader{}, &mockCheckResolver{}, nil)
	require.Error(t, err)
}

func TestRunInParallel(t *testing.T) {
	fn1 := func() (int, error) {
		time.Sleep(10 * time.Microsecond)
		return 1, nil
	}
	fn2 := func() (int, error) {
		time.Sleep(20 * time.Microsecond)
		return 2, nil
	}
	lat1, lat2, res1, res2, err1, err2 := runInParallel(fn1, fn2)
	assert.Equal(t, 1, res1)
	assert.Equal(t, 2, res2)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.GreaterOrEqual(t, lat1, 10*time.Microsecond)
	assert.GreaterOrEqual(t, lat2, 20*time.Microsecond)
}
