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

package grpcproxy

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/requestutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	retryableCode      typeutil.Set[int32]
	fullMethodName2Tag *typeutil.ConcurrentMap[string, string]
	sf                 conc.Singleflight[string]
)

func init() {
	retryableCode = typeutil.NewSet(
		merr.Code(merr.ErrServiceRateLimit),
		merr.Code(merr.ErrCollectionSchemaMismatch),
	)

	fullMethodName2Tag = typeutil.NewConcurrentMap[string, string]()
}

// UnaryRequestStatsInterceptor implements `grpc.UnaryServerInterceptor`
// it records incoming grpc request metrics in unified interceptor
//
// when some retirable error occurs, it will record it as `RetryLabel` instead of failure one
// when other interceptor rejects the request, it will record it as `RejectedLabel`
func UnaryRequestStatsInterceptor(ctx context.Context, req any, rpcInfo *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	methodTag := FullMethodName2Tag(rpcInfo.FullMethod)
	db, _ := requestutil.GetDbNameFromRequest(req)
	collection, _ := requestutil.GetCollectionNameFromRequest(req)

	dbName := db.(string)
	collectionName := collection.(string)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		methodTag,
		metrics.TotalLabel,
		dbName,
		collectionName,
	).Inc()

	start := time.Now()
	resp, err := handler(ctx, req)
	label := ParseMetricLabel(resp, err)

	// set metrics for state code
	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		methodTag,
		label,
		dbName,
		collectionName,
	).Inc()

	// set metrics for latency
	metrics.ProxyGRPCLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		methodTag,
		label,
	).Observe(float64(time.Since(start).Milliseconds()))

	return resp, err
}

// FullMethodName2Tag returns method tag for grpc full method name
// it utilizes `fullMethodName2Tag` as cache result
// if cache miss, it will call `ParseShortMethodName` to parse method tag
// SingleFlight `sf` will make sure there is only one call.
func FullMethodName2Tag(fullMethodName string) string {
	tag, ok := fullMethodName2Tag.Get(fullMethodName)
	if ok {
		return tag
	}

	tag, _, _ = sf.Do(fullMethodName, func() (string, error) {
		tag = ParseShortMethodName(fullMethodName)
		fullMethodName2Tag.Insert(fullMethodName, tag)
		return tag, nil
	})
	return tag
}

// ParseShortMethodName parse short method name from full method name
// input like: "/milvus.proto.milvus.MilvusService/Search"
// returns "Search"
func ParseShortMethodName(fullMethodName string) string {
	parts := strings.Split(fullMethodName, "/")
	return parts[len(parts)-1]
}

// ParseMetricLabel determines the final Prometheus status label based on response and error.
// It implements Scheme Two: using composite labels (fail_input, fail_system) for precision.
func ParseMetricLabel(resp any, err error) string {
	// err only returned by interceptors (e.g., context cancellation, flow control, transport issues)
	if err != nil {
		// Scheme Two: Map generic interceptor errors (often immediate rejection/flow control)
		// to the new precise system rejection label, replacing the old metrics.RejectedLabel.
		return metrics.RejectedSystemLabel
	}

	// Check response status code
	var status *commonpb.Status
	switch resp := resp.(type) {
	case interface{ GetStatus() *commonpb.Status }:
		status = resp.GetStatus()
	case *commonpb.Status:
		status = resp
	}

	// Check if retry/fail
	if !merr.Ok(status) {
		// Check if retry (Logic preserved)
		// NOTE: Assuming retryableCode is available and implemented correctly.
		if retryableCode.Contain(status.GetCode()) {
			return metrics.RetryLabel
		}
		// 1. Convert commonpb.Status to a classified error object (merr.MilvusError).
		//    merr.Error(status) injects classification info based on status.ExtraInfo[InputErrorFlagKey].
		classifiedErr := merr.Error(status)
		var classifier merr.ErrorClassifier
		// 2. Use errors.As to extract the classifier interface.
		if errors.As(classifiedErr, &classifier) {
			if classifier.GetErrorType() == merr.InputError {
				// Hard failure: user input/argument error
				return metrics.FailInputLabel
			}
			// Default: Hard failure: internal system error (merr.SystemError)
			return metrics.FailSystemLabel
		}
		// 3. Fallback: If merr.Error returns an unclassified error (should not happen in theory)
		return metrics.FailSystemLabel
	}
	return metrics.SuccessLabel
}
