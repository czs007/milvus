package datacoord

import (
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/samber/lo"

	"context"
	"go.uber.org/zap"
	"time"
)

type l0CompactionTask struct {
	*compactionTask
}

var _ CompactionTask = (*l0CompactionTask)(nil)

func (t *l0CompactionTask) ShadowClone(opts ...CompactionTaskOpt) CompactionTask {
	task := &l0CompactionTask{
		compactionTask: t.compactionTask,
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

func (t *l0CompactionTask) Process() bool {
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

func (t *l0CompactionTask) processPipelining() bool {
	if t.NeedReAssignNodeID() {
		return false
	}
	err := t.refreshPlan()
	if err != nil {
		t.state = failed
		return true
	}
	t.state = executing
	return false
}

func (t *l0CompactionTask) saveSegmentMeta() bool {
	result := t.result
	plan := t.GetPlan()
	var operators []UpdateOperator
	for _, seg := range result.GetSegments() {
		operators = append(operators, AddBinlogsOperator(seg.GetSegmentID(), nil, nil, seg.GetDeltalogs()))
	}

	levelZeroSegments := lo.Filter(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) bool {
		return b.GetLevel() == datapb.SegmentLevel_L0
	})

	for _, seg := range levelZeroSegments {
		operators = append(operators, UpdateStatusOperator(seg.GetSegmentID(), commonpb.SegmentState_Dropped), UpdateCompactedOperator(seg.GetSegmentID()))
	}

	log.Info("meta update: update segments info for level zero compaction",
		zap.Int64("planID", plan.GetPlanID()),
	)
	err := t.meta.UpdateSegmentsInfo(operators...)
	if err != nil {
		log.Info("Failed to saveSegmentMeta for compaction tasks to DataNode", zap.Error(err))
		return false
	}
	return true
}

func (t *l0CompactionTask) processExecuting() bool {

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

func (t *l0CompactionTask) refreshPlan() error {
	log := log.With(zap.Int64("taskID", t.GetSignalID()), zap.Int64("planID", t.GetPlanID()))
	// Fill in deltalogs for L0 segments
	plan := t.GetPlan()
	for _, seg := range plan.GetSegmentBinlogs() {
		if seg.GetLevel() == datapb.SegmentLevel_L0 {
			segInfo := t.meta.GetHealthySegment(seg.GetSegmentID())
			if segInfo == nil {
				return merr.WrapErrSegmentNotFound(seg.GetSegmentID())
			}
			seg.Deltalogs = segInfo.GetDeltalogs()
		}
	}

	// Select sealed L1 segments for LevelZero compaction that meets the condition:
	// dmlPos < triggerInfo.pos
	// TODO: select L2 segments too
	sealedSegments := t.meta.SelectSegments(WithCollection(t.triggerInfo.collectionID), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return (t.triggerInfo.partitionID == -1 || info.GetPartitionID() == t.triggerInfo.partitionID) &&
			info.GetInsertChannel() == plan.GetChannel() &&
			isFlushState(info.GetState()) &&
			!info.isCompacting &&
			!info.GetIsImporting() &&
			info.GetLevel() != datapb.SegmentLevel_L0 &&
			info.GetDmlPosition().GetTimestamp() < t.triggerInfo.pos.GetTimestamp()
	}))
	if len(sealedSegments) == 0 {
		return errors.Errorf("Selected zero L1/L2 segments for the position=%v", t.triggerInfo.pos)
	}

	sealedSegBinlogs := lo.Map(sealedSegments, func(info *SegmentInfo, _ int) *datapb.CompactionSegmentBinlogs {
		return &datapb.CompactionSegmentBinlogs{
			SegmentID:    info.GetID(),
			Level:        info.GetLevel(),
			CollectionID: info.GetCollectionID(),
			PartitionID:  info.GetPartitionID(),
		}
	})

	plan.SegmentBinlogs = append(plan.SegmentBinlogs, sealedSegBinlogs...)
	log.Info("Compaction handler refreshed level zero compaction plan",
		zap.Any("target position", t.triggerInfo.pos),
		zap.Any("target segments count", len(sealedSegBinlogs)))
	return nil
}

func (t *l0CompactionTask) processMetaSaved() bool {
	t.resetSegmentCompacting()
	UpdateCompactionSegmentSizeMetrics(t.result.GetSegments())
	t.state = completed
	log.Info("l0CompactionTask: success to handle merge compaction result")
	return true
}
