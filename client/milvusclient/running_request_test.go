// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

type RunningRequestSuite struct {
	MockSuiteBase
}

func runningRequestInfo(id int64, state string) *milvuspb.RunningRequestInfo {
	return &milvuspb.RunningRequestInfo{
		RequestId:      id,
		ProxyId:        7,
		Type:           "Search",
		DbName:         "default",
		CollectionName: "coll",
		User:           "alice",
		ClientAddr:     "10.0.0.1:5000",
		Nq:             10,
		Topk:           100,
		Expr:           "id > 0",
		StartTimeMs:    1700000000000,
		QueuedMs:       3,
		ElapsedMs:      2500,
		State:          state,
		TaskIds:        []int64{11, 12},
		TraceId:        "abc",
		Cancellable:    true,
	}
}

func (s *RunningRequestSuite) TestListRunningRequests() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("filters_and_rows", func() {
		s.mock.EXPECT().ListRunningRequests(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ListRunningRequestsRequest) (*milvuspb.ListRunningRequestsResponse, error) {
			s.Equal("db1", req.GetDbName())
			s.Equal("coll", req.GetCollectionName())
			s.Equal("alice", req.GetUser())
			s.EqualValues(1500, req.GetMinElapsedMs())
			return &milvuspb.ListRunningRequestsResponse{
				Status:   merr.Success(),
				Requests: []*milvuspb.RunningRequestInfo{runningRequestInfo(1, "Running"), runningRequestInfo(2, "Canceling")},
				NodeResults: []*milvuspb.RunningRequestNodeResult{
					{NodeId: 7, Status: merr.Success()},
					{NodeId: 8, Status: merr.Status(merr.WrapErrServiceInternal("deadline exceeded"))},
					{NodeId: 9, Status: &commonpb.Status{Code: 1, Reason: "unimplemented"}, Unimplemented: true},
				},
			}, nil
		}).Once()

		result, err := s.client.ListRunningRequests(ctx, NewListRunningRequestsOption().
			WithDBName("db1").WithCollectionName("coll").WithUser("alice").WithMinElapsed(1500*time.Millisecond))
		s.NoError(err)
		s.Require().Len(result.Requests, 2)
		first := result.Requests[0]
		s.EqualValues(1, first.ID)
		s.EqualValues(7, first.ProxyID)
		s.Equal("Search", first.Type)
		s.Equal("default", first.DBName)
		s.Equal("coll", first.CollectionName)
		s.Equal("alice", first.User)
		s.Equal("10.0.0.1:5000", first.ClientAddr)
		s.EqualValues(10, first.NQ)
		s.EqualValues(100, first.TopK)
		s.Equal("id > 0", first.Expr)
		s.Equal(time.UnixMilli(1700000000000), first.StartTime)
		s.Equal(3*time.Millisecond, first.Queued)
		s.Equal(2500*time.Millisecond, first.Elapsed)
		s.Equal("Running", first.State)
		s.Equal([]int64{11, 12}, first.TaskIDs)
		s.Equal("abc", first.TraceID)
		s.True(first.Cancellable)
		s.Equal("Canceling", result.Requests[1].State)

		s.Require().Len(result.NodeResults, 3)
		s.EqualValues(7, result.NodeResults[0].NodeID)
		s.NoError(result.NodeResults[0].Err)
		s.False(result.NodeResults[0].Unimplemented)
		s.EqualValues(8, result.NodeResults[1].NodeID)
		s.ErrorContains(result.NodeResults[1].Err, "deadline exceeded")
		s.False(result.NodeResults[1].Unimplemented)
		s.EqualValues(9, result.NodeResults[2].NodeID)
		s.Error(result.NodeResults[2].Err)
		s.True(result.NodeResults[2].Unimplemented)
	})

	s.Run("no_filter", func() {
		s.mock.EXPECT().ListRunningRequests(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ListRunningRequestsRequest) (*milvuspb.ListRunningRequestsResponse, error) {
			s.Empty(req.GetDbName())
			s.Empty(req.GetCollectionName())
			s.Empty(req.GetUser())
			s.Zero(req.GetMinElapsedMs())
			return &milvuspb.ListRunningRequestsResponse{Status: merr.Success()}, nil
		}).Once()

		result, err := s.client.ListRunningRequests(ctx, NewListRunningRequestsOption())
		s.NoError(err)
		s.Empty(result.Requests)
		s.Empty(result.NodeResults)
	})

	s.Run("rpc_failure", func() {
		s.mock.EXPECT().ListRunningRequests(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListRunningRequests(ctx, NewListRunningRequestsOption())
		s.Error(err)
	})

	s.Run("status_failure", func() {
		s.mock.EXPECT().ListRunningRequests(mock.Anything, mock.Anything).Return(&milvuspb.ListRunningRequestsResponse{
			Status: merr.Status(merr.WrapErrServiceInternal("permission denied")),
		}, nil).Once()

		_, err := s.client.ListRunningRequests(ctx, NewListRunningRequestsOption())
		s.ErrorContains(err, "permission denied")
	})
}

func (s *RunningRequestSuite) TestCancelRequests() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("sorted_ids", func() {
		s.mock.EXPECT().CancelRequests(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.CancelRequestsRequest) (*milvuspb.CancelRequestsResponse, error) {
			s.Equal([]int64{1, 2, 3}, req.GetRequestIds())
			s.Equal("too slow", req.GetReason())
			s.Equal("alice", req.GetUser())
			return &milvuspb.CancelRequestsResponse{
				Status:       merr.Success(),
				Canceled:     []*milvuspb.RunningRequestInfo{runningRequestInfo(1, "Running")},
				NotFound:     []int64{2},
				Undetermined: []int64{3},
				NodeResults: []*milvuspb.RunningRequestNodeResult{
					{NodeId: 7, Status: merr.Success()},
					{NodeId: 8, Status: merr.Status(merr.WrapErrServiceInternal("unreachable"))},
				},
			}, nil
		}).Once()

		result, err := s.client.CancelRequests(ctx, NewCancelRequestsOption(1, 2, 3).WithReason("too slow").WithUser("alice"))
		s.NoError(err)
		s.Require().Len(result.Canceled, 1)
		s.EqualValues(1, result.Canceled[0].ID)
		s.Equal("Running", result.Canceled[0].State)
		s.Equal([]int64{2}, result.NotFound)
		s.Equal([]int64{3}, result.Undetermined)
		s.Require().Len(result.NodeResults, 2)
		s.NoError(result.NodeResults[0].Err)
		s.ErrorContains(result.NodeResults[1].Err, "unreachable")
	})

	s.Run("empty_ids_rejected_by_server", func() {
		s.mock.EXPECT().CancelRequests(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.CancelRequestsRequest) (*milvuspb.CancelRequestsResponse, error) {
			s.Empty(req.GetRequestIds())
			return &milvuspb.CancelRequestsResponse{
				Status: merr.Status(merr.WrapErrParameterMissingMsg("request_ids cannot be empty")),
			}, nil
		}).Once()

		_, err := s.client.CancelRequests(ctx, NewCancelRequestsOption())
		s.ErrorContains(err, "request_ids cannot be empty")
	})

	s.Run("rpc_failure", func() {
		s.mock.EXPECT().CancelRequests(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.CancelRequests(ctx, NewCancelRequestsOption(1))
		s.Error(err)
	})
}

func TestRunningRequests(t *testing.T) {
	suite.Run(t, new(RunningRequestSuite))
}
