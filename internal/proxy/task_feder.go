package proxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/federpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"go.uber.org/zap"
)

const (
	FederDescribeSegmentIndexData = "FederDescribeSegmentIndexData"
)

type federDescribeSegmentIndexDataTask struct {
	request *federpb.DescribeSegmentIndexDataRequest
	result  *federpb.DescribeSegmentIndexDataResponse
	Condition
	collectionName string

	ctx             context.Context
	dc              types.DataCoord
	tr              *timerecord.TimeRecorder
//	toReduceResults []

	*internalpb.FederDescribeSegmentIndexDataRequest
	qc types.QueryCoord

	shardMgr             *shardClientMgr
}

func (g *federDescribeSegmentIndexDataTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *federDescribeSegmentIndexDataTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *federDescribeSegmentIndexDataTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *federDescribeSegmentIndexDataTask) Name() string {
	return FederDescribeSegmentIndexData
}

func (g *federDescribeSegmentIndexDataTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *federDescribeSegmentIndexDataTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *federDescribeSegmentIndexDataTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *federDescribeSegmentIndexDataTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *federDescribeSegmentIndexDataTask) OnEnqueue() error {
	g.FederDescribeSegmentIndexDataRequest = &internalpb.FederDescribeSegmentIndexDataRequest{
		Base: commonpbutil.NewMsgBase(),
	}
	return nil
}

func (g *federDescribeSegmentIndexDataTask) PreExecute(ctx context.Context) error {
	return nil
}

func (g *federDescribeSegmentIndexDataTask) Execute(ctx context.Context) error {
	return nil
}

func (g *federDescribeSegmentIndexDataTask) PostExecute(ctx context.Context) error {
	return nil
}
