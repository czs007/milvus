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

package datacoord

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type compactionPlanContext interface {
	start()
	stop()
	// execCompactionPlan start to execute plan and return immediately
	execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error
	// isFull return true if the task pool is full
	isFull() bool
	// get compaction tasks by signal id
	//getCompactionTasksBySignalID(signalID int64) []CompactionTask
	getCompactionTasksNumBySignalID(signalID int64) int
	getCompactionInfo(signalID int64) *compactionInfo
	removeTasksByChannel(channel string)
}

type compactionTaskState int8

const (
	executing compactionTaskState = iota + 1
	pipelining
	completed
	failed
	timeout
	metaSaved
)

var (
	errChannelNotWatched = errors.New("channel is not watched")
	errChannelInBuffer   = errors.New("channel is in buffer")
)

type CompactionMeta interface {
	SelectSegments(filters ...SegmentFilter) []*SegmentInfo
	GetHealthySegment(segID UniqueID) *SegmentInfo
	UpdateSegmentsInfo(operators ...UpdateOperator) error
	SetSegmentCompacting(segmentID int64, compacting bool)

	CompleteCompactionMutation(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error)
}

var _ CompactionMeta = (*meta)(nil)

var _ compactionPlanContext = (*compactionPlanHandler)(nil)

type compactionInfo struct {
	state        commonpb.CompactionState
	executingCnt int
	completedCnt int
	failedCnt    int
	timeoutCnt   int
	mergeInfos   map[int64]*milvuspb.CompactionMergeInfo
}

type compactionPlanHandler struct {
	mu         sync.RWMutex
	queueTasks map[int64]CompactionTask // planID -> task

	executingMu    sync.RWMutex
	executingTasks map[int64]CompactionTask // planID -> task

	meta      CompactionMeta
	allocator allocator
	chManager ChannelManager
	sessions  SessionManager
	cluster   Cluster

	stopCh   chan struct{}
	stopOnce sync.Once
	stopWg   sync.WaitGroup

	taskNumber *atomic.Int32
}

func (c *compactionPlanHandler) getCompactionInfo(signalID int64) *compactionInfo {
	var executingCnt int
	var completedCnt int
	var failedCnt int
	var timeoutCnt int
	ret := &compactionInfo{}

	mergeInfos := make(map[int64]*milvuspb.CompactionMergeInfo)

	c.mu.RLock()
	for _, t := range c.queueTasks {
		if t.GetSignalID() == signalID {
			executingCnt += 1
			mergeInfos[t.GetPlanID()] = getCompactionMergeInfo(t)
		}
	}
	//panic("implement me")
	c.mu.RUnlock()

	c.executingMu.RLock()
	for _, t := range c.queueTasks {
		if t.GetSignalID() == signalID {
			switch t.GetState() {
			case pipelining, executing, metaSaved:
				executingCnt++
			case completed:
				completedCnt++
			case failed:
				failedCnt++
			case timeout:
				timeoutCnt++
			}
			mergeInfos[t.GetPlanID()] = getCompactionMergeInfo(t)
		}
	}

	ret.executingCnt = executingCnt
	ret.completedCnt = completedCnt
	ret.timeoutCnt = timeoutCnt
	ret.failedCnt = failedCnt

	if executingCnt != 0 {
		ret.state = commonpb.CompactionState_Executing
	} else {
		ret.state = commonpb.CompactionState_Completed
	}
	return ret
}

func (c *compactionPlanHandler) getCompactionTasksNumBySignalID(signalID int64) int {
	cnt := 0
	c.mu.RLock()
	for _, t := range c.queueTasks {
		if t.GetSignalID() == signalID {
			cnt += 1
		}
		//if t.GetPlanID()
	}
	cnt += len(c.queueTasks)
	c.mu.RUnlock()
	c.executingMu.RLock()
	for _, t := range c.executingTasks {
		if t.GetSignalID() == signalID {
			cnt += 1
		}
	}
	cnt += len(c.queueTasks)
	c.executingMu.RUnlock()
	return cnt
}

func newCompactionPlanHandler(cluster Cluster, sessions SessionManager, cm ChannelManager, meta CompactionMeta, allocator allocator,
) *compactionPlanHandler {
	return &compactionPlanHandler{
		queueTasks: make(map[int64]CompactionTask),
		chManager:  cm,
		meta:       meta,
		sessions:   sessions,
		allocator:  allocator,
		stopCh:     make(chan struct{}),
		cluster:    cluster,
	}
}

func (c *compactionPlanHandler) schedule() {
	c.mu.RLock()
	if len(c.queueTasks) == 0 {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()

	l0ChannelExcludes := typeutil.NewSet[string]()
	mixChannelExcludes := typeutil.NewSet[string]()
	//clusterChannelExcludes := typeutil.NewSet[string]()
	mixLabelExcludes := typeutil.NewSet[string]()
	//clusterLabelExcludes := typeutil.NewSet[string]()
	for _, t := range c.executingTasks {
		switch t.GetPlan().GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			l0ChannelExcludes.Insert(t.GetChannel())
		case datapb.CompactionType_MixCompaction:
			mixChannelExcludes.Insert(t.GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
			//case datapb.CompactionType_ClusteringCompaction:
			//	clusterChannelExcludes.Insert(t.GetChannel())
			//	clusterLabelExcludes.Insert(t.GetLabel())
		}
	}

	picked := []CompactionTask{}
	c.mu.RLock()
	for _, t := range c.queueTasks {
		switch t.GetType() {
		case datapb.CompactionType_Level0DeleteCompaction:
			if l0ChannelExcludes.Contain(t.GetChannel()) ||
				mixChannelExcludes.Contain(t.GetChannel()) {
				continue
			}
			picked = append(picked, t)
			l0ChannelExcludes.Insert(t.GetChannel())
		case datapb.CompactionType_MixCompaction:
			if l0ChannelExcludes.Contain(t.GetChannel()) {
				continue
			}
			picked = append(picked, t)
			mixChannelExcludes.Insert(t.GetChannel())
			mixLabelExcludes.Insert(t.GetLabel())
			//case datapb.CompactionType_ClusteringCompaction:
			//	if l0ChannelExcludes.Contain(t.GetChannel()) ||
			//		mixLabelExcludes.Contain(t.GetLabel()) ||
			//		clusterLabelExcludes.Contain(t.GetLabel()){
			//		continue
			//	}
			//	picked = append(picked, t)
			//	slot -= 1
			//	clusterChannelExcludes.Insert(t.GetChannel())
			//	clusterLabelExcludes.Insert(t.GetLabel())
		}
	}
	c.mu.RUnlock()
	if len(picked) > 0 {
		c.executingMu.Lock()
		for _, t := range picked {
			t.SetStartTime(time.Now().Unix())
			c.executingTasks[t.GetPlanID()] = t
		}
		c.executingMu.Unlock()
	}
}

func (c *compactionPlanHandler) start() {
	c.stopWg.Add(2)
	go c.loopSchedule()
	go c.loopCheck()
}

func (c *compactionPlanHandler) loopSchedule() {
	log.Info("compactionPlanHandler start loop schedule")
	defer c.stopWg.Done()

	scheduleTicker := time.NewTicker(200 * time.Millisecond)
	defer scheduleTicker.Stop()
	for {
		select {
		case <-c.stopCh:
			log.Info("compactionPlanHandler quit loop schedule")
			return

		case <-scheduleTicker.C:
			c.schedule()
		}
	}
}

func (c *compactionPlanHandler) loopCheck() {
	interval := Params.DataCoordCfg.CompactionCheckIntervalInSeconds.GetAsDuration(time.Second)
	log.Info("compactionPlanHandler start loop check", zap.Any("check result interval", interval))
	defer c.stopWg.Done()
	checkResultTicker := time.NewTicker(interval)
	for {
		select {
		case <-c.stopCh:
			log.Info("compactionPlanHandler quit loop check")
			return

		case <-checkResultTicker.C:
			err := c.checkCompaction()
			if err != nil {
				log.Info("fail to update compaction", zap.Error(err))
			}
		}
	}
}

func (c *compactionPlanHandler) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
	c.stopWg.Wait()
}

func (c *compactionPlanHandler) removeTasksByChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, task := range c.queueTasks {
		if task.GetChannel() == channel {
			log.Info("Compaction handler removing tasks by channel",
				zap.String("channel", channel),
				zap.Int64("planID", task.GetPlanID()),
				zap.Int64("node", task.GetNodeID()),
			)
			delete(c.queueTasks, id)
		}
	}
}

func (c *compactionPlanHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
	nodeID, err := c.chManager.FindWatcher(plan.GetChannel())
	if err != nil {
		log.Error("failed to find watcher", zap.Int64("planID", plan.GetPlanID()), zap.Error(err))
		return err
	}

	log := log.With(zap.Int64("planID", plan.GetPlanID()), zap.Int64("nodeID", nodeID))
	c.setSegmentsCompacting(plan, true)

	_, span := otel.Tracer(typeutil.DataCoordRole).Start(context.Background(), fmt.Sprintf("Compaction-%s", plan.GetType()))

	var task CompactionTask
	label := CompactionGroupLabel{
		CollectionID: signal.collectionID,
		PartitionID:  signal.partitionID,
		Channel:      signal.channel}

	if plan.GetType() == datapb.CompactionType_MixCompaction {
		task = &mixCompactionTask{
			compactionTask: &compactionTask{
				triggerInfo:    signal,
				CompactionPlan: plan,
				span:           span,
				state:          pipelining,
				labelString:    label.Key(),
				sessions:       c.sessions,
				meta:           c.meta,
				nodeID:         nodeID,
			},
		}
	} else if plan.GetType() == datapb.CompactionType_Level0DeleteCompaction {
		task = &l0CompactionTask{
			compactionTask: &compactionTask{
				triggerInfo:    signal,
				CompactionPlan: plan,
				span:           span,
				state:          pipelining,
				labelString:    label.Key(),
				sessions:       c.sessions,
				meta:           c.meta,
				nodeID:         nodeID,
			},
		}
	}
	c.mu.Lock()
	c.queueTasks[plan.PlanID] = task
	c.mu.Unlock()

	log.Info("Compaction plan submited")
	return nil
}

func (c *compactionPlanHandler) setSegmentsCompacting(plan *datapb.CompactionPlan, compacting bool) {
	for _, segmentBinlogs := range plan.GetSegmentBinlogs() {
		c.meta.SetSegmentCompacting(segmentBinlogs.GetSegmentID(), compacting)
	}
}

func (c *compactionPlanHandler) assignNodeIDs(tasks []CompactionTask) {

	//slots := c.cluster.QuerySlots()
	//if len(slots) == 0 {
	//	return
	//}
	for _, t := range tasks {
		nodeID, err := c.chManager.FindWatcher(t.GetChannel())
		if err != nil {
			log.Info("failed to find watcher", zap.Int64("planID", t.GetPlanID()), zap.Error(err))
			continue
		}
		t.SetNodeID(nodeID)
	}

}

func (c *compactionPlanHandler) checkCompaction() error {
	// Get executing executingTasks before GetCompactionState from DataNode to prevent false failure,
	//  for DC might add new task while GetCompactionState.

	var needAssignIDTasks []CompactionTask
	c.executingMu.RLock()
	for _, t := range c.executingTasks {
		if t.NeedReAssignNodeID() {
			needAssignIDTasks = append(needAssignIDTasks, t)
		}
	}
	c.executingMu.RUnlock()
	c.assignNodeIDs(needAssignIDTasks)

	var finishedTasks []CompactionTask
	c.executingMu.RLock()
	for _, t := range c.executingTasks {
		finished := t.Process()
		if finished {
			finishedTasks = append(finishedTasks, t)
		}
	}
	c.executingMu.RUnlock()

	// delete all finished
	c.executingMu.Lock()
	for _, t := range finishedTasks {
		delete(c.executingTasks, t.GetPlanID())
	}
	c.executingMu.Unlock()
	return nil
}

// isFull return true if the task pool is full
func (c *compactionPlanHandler) isFull() bool {
	return int(c.taskNumber.Load()) >= Params.DataCoordCfg.CompactionMaxParallelTasks.GetAsInt()
}

func (c *compactionPlanHandler) getTasksByState(state compactionTaskState) []CompactionTask {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tasks := make([]CompactionTask, 0, len(c.queueTasks))
	for _, t := range c.queueTasks {
		if t.GetState() == state {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

var (
	ioPool         *conc.Pool[any]
	ioPoolInitOnce sync.Once
)

func initIOPool() {
	capacity := Params.DataNodeCfg.IOConcurrency.GetAsInt()
	if capacity > 32 {
		capacity = 32
	}
	// error only happens with negative expiry duration or with negative pre-alloc size.
	ioPool = conc.NewPool[any](capacity)
}

func getOrCreateIOPool() *conc.Pool[any] {
	ioPoolInitOnce.Do(initIOPool)
	return ioPool
}
