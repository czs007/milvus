package datacoord

import (
	"context"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
	"time"
)

type mixCompactionTask struct {
	*compactionTask
	finished   bool
	newSegment *SegmentInfo
}

var _ CompactionTask = (*mixCompactionTask)(nil)

func (t *mixCompactionTask) ShadowClone(opts ...CompactionTaskOpt) CompactionTask {
	task := &mixCompactionTask{
		compactionTask: t.compactionTask,
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

func (t *mixCompactionTask) processPipelining() bool {
	err := t.refreshPlan()
	if err != nil {
		t.state = failed
		return true
	}

	err = t.sessions.Compaction(context.Background(), t.GetNodeID(), t.GetPlan())
	if err != nil {
		log.Warn("Failed to notify compaction tasks to DataNode", zap.Error(err))
		t.state = pipelining
		t.nodeID = 0
		return false
	}
	t.state = executing
	return false
}

func (t *mixCompactionTask) processMetaSaved() bool {
	nodeID := t.GetNodeID()
	plan := t.GetPlan()
	newSegmentInfo := t.newSegment
	req := &datapb.SyncSegmentsRequest{
		PlanID:        plan.PlanID,
		CompactedTo:   newSegmentInfo.GetID(),
		CompactedFrom: newSegmentInfo.GetCompactionFrom(),
		NumOfRows:     newSegmentInfo.GetNumOfRows(),
		StatsLogs:     newSegmentInfo.GetStatslogs(),
		ChannelName:   plan.GetChannel(),
		PartitionId:   newSegmentInfo.GetPartitionID(),
		CollectionId:  newSegmentInfo.GetCollectionID(),
	}

	//TODO Remove SyncSegments
	log.Info("handleCompactionResult: syncing segments with node", zap.Int64("nodeID", nodeID))
	if err := t.sessions.SyncSegments(nodeID, req); err != nil {
		log.Warn("handleCompactionResult: fail to sync segments with node",
			zap.Int64("nodeID", nodeID), zap.Error(err))
		return false
	}
	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())

	t.state = completed

	log.Info("handleCompactionResult: success to handle merge compaction result")
	return true
}

func (t *mixCompactionTask) processExecuting() bool {
	session, ok := t.sessions.GetSession(t.GetNodeID())
	if !ok {
		t.state = pipelining
		t.nodeID = 0
		return false
	}
	cli, err := session.GetOrCreateClient(context.Background())
	if err != nil {
		log.Info("Cannot Create Client", zap.Int64("NodeID", t.GetNodeID()))
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), Params.DataCoordCfg.CompactionRPCTimeout.GetAsDuration(time.Second))
	defer cancel()
	resp, err2 := cli.GetCompactionState(ctx, &datapb.CompactionStateRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetSystemConfigs),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		PlanID: t.GetPlanID(),
	})

	if err2 != nil {
		return false
	}

	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Info("Get CompState failed", zap.Error(err))
		return false
	}
	var result *datapb.CompactionPlanResult
	for _, rst := range resp.GetResults() {
		if rst.GetPlanID() != t.GetPlanID() {
			continue
		}
		result = rst
		break
	}
	switch result.GetState() {
	case commonpb.CompactionState_Executing:
		if t.checkTimeout() {
			t.state = timeout
			return t.processTimeout()
		}
		return false
	case commonpb.CompactionState_Completed:
		t.result = result
		saveSuccess := t.saveSegmentMeta()
		if !saveSuccess {
			return false
		}
		t.state = metaSaved
		return t.processMetaSaved()
	}
	return false
}

func (t *mixCompactionTask) saveSegmentMeta() bool {
	result := t.result
	if len(result.GetSegments()) == 0 || len(result.GetSegments()) > 1 {
		log.Warn("illegal compaction results")
		t.state = failed
		return false
	}

	// Also prepare metric updates.
	newSegments, metricMutation, err2 := t.meta.CompleteCompactionMutation(t.GetPlan(), result)
	if err2 != nil {
		return false
	}
	// Apply metrics after successful meta update.
	t.newSegment = newSegments[0]
	metricMutation.commit()
	return true
}

func (t *mixCompactionTask) Process() bool {
	switch t.state {
	case pipelining:
		return t.processPipelining()
	case executing:
		return t.processExecuting()
	case timeout:
		return t.processTimeout()
	case metaSaved:
		return t.processMetaSaved()
	case completed:
		return t.processCompleted()
	}
	return true
}

func (t *mixCompactionTask) refreshPlan() error {
	log := log.With(zap.Int64("taskID", t.GetSignalID()), zap.Int64("planID", t.GetPlanID()))
	plan := t.GetPlan()
	segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
	for _, seg := range plan.GetSegmentBinlogs() {
		info := t.meta.GetHealthySegment(seg.GetSegmentID())
		if info == nil {
			return merr.WrapErrSegmentNotFound(seg.GetSegmentID())
		}
		seg.Deltalogs = info.GetDeltalogs()
		segIDMap[seg.SegmentID] = info.GetDeltalogs()
	}
	log.Info("Compaction handler refreshed mix compaction plan", zap.Any("segID2DeltaLogs", segIDMap))
	return nil
}
