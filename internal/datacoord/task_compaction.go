package datacoord

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.opentelemetry.io/otel/trace"

	"go.uber.org/zap"
	"time"
)

type CompactionTask interface {
	Process() bool
	GetPlanID() UniqueID
	GetChannel() string
	GetLabel() string
	SetStartTime(startTime int64)
	GetType() datapb.CompactionType
	GetSignalID() UniqueID
	GetState() compactionTaskState
	ShadowClone(opts ...CompactionTaskOpt) CompactionTask
	GetNodeID() UniqueID
	SetNodeID(UniqueID)
	GetPlan() *datapb.CompactionPlan
	NeedReAssignNodeID() bool
	GetResult() *datapb.CompactionPlanResult
}

type compactionTask struct {
	triggerInfo *compactionSignal
	*datapb.CompactionPlan
	state       compactionTaskState
	nodeID      int64
	result      *datapb.CompactionPlanResult
	span        trace.Span
	labelString string
	sessions    SessionManager
	meta        CompactionMeta
}

func (t *compactionTask) GetResult() *datapb.CompactionPlanResult {
	return t.result
}

func (t *compactionTask) GetPlan() *datapb.CompactionPlan {
	return t.CompactionPlan
}

func (t *compactionTask) GetNodeID() UniqueID {
	return t.nodeID
}

func (t *compactionTask) GetState() compactionTaskState {
	return t.state
}

func (t *compactionTask) GetSignalID() UniqueID {
	return t.triggerInfo.id
}

func (t *compactionTask) GetLabel() string {
	return t.labelString
}

func (t *compactionTask) SetStartTime(startTime int64) {
	t.StartTime = startTime
}

func (t *compactionTask) NeedReAssignNodeID() bool {
	return t.state == pipelining && t.nodeID == 0
}

func (t *compactionTask) processCompleted() bool {
	for _, segmentBinlogs := range t.GetPlan().GetSegmentBinlogs() {
		t.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), false)
	}
	return true
}

func (t *compactionTask) resetSegmentCompacting() {
	for _, segmentBinlogs := range t.GetPlan().GetSegmentBinlogs() {
		t.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), false)
	}
}

func (t *compactionTask) processTimeout() bool {
	t.resetSegmentCompacting()
	return true
}

func (t *compactionTask) checkTimeout() bool {
	if t.GetPlan().GetTimeoutInSeconds() > 0 {
		diff := time.Since(time.Unix(t.GetPlan().GetStartTime(), 0)).Seconds()
		if diff > float64(t.GetPlan().GetTimeoutInSeconds()) {
			log.Warn("compaction timeout",
				zap.Int32("timeout in seconds", t.GetPlan().GetTimeoutInSeconds()),
				zap.Int64("startTime", t.GetPlan().GetStartTime()),
			)
			return true
		}
	}
	return false
}

func (t *compactionTask) SetNodeID(id UniqueID) {
	t.nodeID = id
}

type CompactionTaskOpt func(task CompactionTask)

func setStartTime(startTime int64) CompactionTaskOpt {
	return func(task CompactionTask) {
		task.SetStartTime(startTime)
	}
}
