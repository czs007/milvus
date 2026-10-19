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

package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/eventlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type LoadCollectionJob struct {
	*BaseJob

	result             message.BroadcastResultAlterLoadConfigMessageV2
	undo               *UndoList
	dist               *meta.DistributionManager
	meta               *meta.Meta
	broker             meta.Broker
	targetMgr          meta.TargetManagerInterface
	targetObserver     *observers.TargetObserver
	collectionObserver *observers.CollectionObserver
	checkerController  *checkers.CheckerController
	nodeMgr            *session.NodeManager
	proxyManager       proxyutil.ProxyClientManagerInterface
	cluster            session.Cluster
}

func NewLoadCollectionJob(
	ctx context.Context,
	result message.BroadcastResultAlterLoadConfigMessageV2,
	dist *meta.DistributionManager,
	meta *meta.Meta,
	broker meta.Broker,
	targetMgr meta.TargetManagerInterface,
	targetObserver *observers.TargetObserver,
	collectionObserver *observers.CollectionObserver,
	checkerController *checkers.CheckerController,
	nodeMgr *session.NodeManager,
	proxyManager proxyutil.ProxyClientManagerInterface,
	cluster session.Cluster,
) *LoadCollectionJob {
	return &LoadCollectionJob{
		BaseJob:            NewBaseJob(ctx, 0, result.Message.Header().GetCollectionId()),
		result:             result,
		undo:               NewUndoList(ctx, meta, targetMgr, targetObserver),
		dist:               dist,
		meta:               meta,
		broker:             broker,
		targetMgr:          targetMgr,
		targetObserver:     targetObserver,
		collectionObserver: collectionObserver,
		checkerController:  checkerController,
		nodeMgr:            nodeMgr,
		proxyManager:       proxyManager,
		cluster:            cluster,
	}
}

func (job *LoadCollectionJob) Execute() error {
	req := job.result.Message.Header()
	log := log.Ctx(job.ctx).With(zap.Int64("collectionID", req.GetCollectionId()))
	meta.GlobalFailedLoadCache.Remove(req.GetCollectionId())

	collInfo, err := job.broker.DescribeCollection(job.ctx, req.GetCollectionId())
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	// 1. resolve replica config: use local cluster-level config if this is a replicated message
	replicas := req.GetReplicas()
	if req.GetUseLocalReplicaConfig() {
		localReplicas, err := getLocalReplicaConfig(job.ctx, job.meta, req.GetCollectionId())
		if err != nil {
			return err
		}
		replicas = localReplicas
		log.Info("using local cluster-level replica config for replicated load",
			zap.Int("localReplicaCount", len(localReplicas)))
	}

	// 2. create replica if not exist (may also remove redundant replicas)
	if _, err := utils.SpawnReplicasWithReplicaConfig(job.ctx, job.meta, meta.SpawnWithReplicaConfigParams{
		CollectionID: req.GetCollectionId(),
		Channels:     collInfo.GetVirtualChannelNames(),
		Configs:      replicas,
	}); err != nil {
		return err
	}

	// 2.1 invalidate shard leader cache after replica changes, so proxies stop
	// routing to released replicas' shard leaders before async cleanup happens.
	if job.proxyManager != nil {
		job.proxyManager.InvalidateShardLeaderCache(job.ctx, &proxypb.InvalidateShardLeaderCacheRequest{
			CollectionIDs: []int64{req.GetCollectionId()},
		})
	}

	// 3. put load info meta
	fieldIndexIDs := make(map[int64]int64, len(req.GetLoadFields()))
	fieldIDs := make([]int64, 0, len(req.GetLoadFields()))
	for _, loadField := range req.GetLoadFields() {
		if loadField.GetIndexId() != 0 {
			fieldIndexIDs[loadField.GetFieldId()] = loadField.GetIndexId()
		}
		fieldIDs = append(fieldIDs, loadField.GetFieldId())
	}
	replicaNumber := int32(len(replicas))
	partitions := lo.Map(req.GetPartitionIds(), func(partID int64, _ int) *meta.Partition {
		return &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID:  req.GetCollectionId(),
				PartitionID:   partID,
				ReplicaNumber: replicaNumber,
				Status:        querypb.LoadStatus_Loading,
				FieldIndexID:  fieldIndexIDs,
			},
			CreatedAt: time.Now(),
		}
	})

	ctx, sp := otel.Tracer(typeutil.QueryCoordRole).Start(job.ctx, "LoadCollection", trace.WithNewRoot())
	collection := &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:             req.GetCollectionId(),
			ReplicaNumber:            replicaNumber,
			Status:                   querypb.LoadStatus_Loading,
			FieldIndexID:             fieldIndexIDs,
			LoadType:                 querypb.LoadType_LoadCollection,
			LoadFields:               fieldIDs,
			DbID:                     req.GetDbId(),
			UserSpecifiedReplicaMode: req.GetUserSpecifiedReplicaMode(),
		},
		CreatedAt: time.Now(),
		LoadSpan:  sp,
		Schema:    collInfo.GetSchema(),
	}
	incomingPartitions := typeutil.NewSet(req.GetPartitionIds()...)
	currentPartitions := job.meta.GetPartitionsByCollection(job.ctx, req.GetCollectionId())
	toReleasePartitions := make([]int64, 0)
	for _, partition := range currentPartitions {
		if !incomingPartitions.Contain(partition.GetPartitionID()) {
			toReleasePartitions = append(toReleasePartitions, partition.GetPartitionID())
		}
	}
	if len(toReleasePartitions) > 0 {
		job.targetObserver.ReleasePartition(req.GetCollectionId(), toReleasePartitions...)
		if err := job.meta.RemovePartition(job.ctx, req.GetCollectionId(), toReleasePartitions...); err != nil {
			return merr.Wrap(err, "failed to remove partitions")
		}
	}

	if err = job.meta.PutCollection(job.ctx, collection, partitions...); err != nil {
		msg := "failed to store collection and partitions"
		log.Warn(msg, zap.Error(err))
		return merr.Wrapf(err, "%s", msg)
	}
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Start load collection %d", collection.CollectionID)))
	metrics.QueryCoordNumPartitions.WithLabelValues().Add(float64(len(partitions)))

	log.Info("put collection and partitions done",
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.Int64s("partitions", req.GetPartitionIds()),
		zap.Int64s("toReleasePartitions", toReleasePartitions),
	)

	// 5. update next target, no need to rollback if pull target failed, target observer will pull target in periodically
	if _, err = job.targetObserver.UpdateNextTarget(job.ctx, req.GetCollectionId()); err != nil {
		return err
	}

	// 6. register load task into collection observer
	job.collectionObserver.LoadPartitions(ctx, req.GetCollectionId(), incomingPartitions.Collect())

	// 7. release the partitions on the delegators, then wait for the release to settle
	if len(toReleasePartitions) > 0 {
		job.releasePartitionsOnDelegators(ctx, req.GetCollectionId(), toReleasePartitions)
		if err = WaitCurrentTargetUpdated(ctx, job.targetObserver, req.GetCollectionId()); err != nil {
			log.Warn("failed to wait current target updated", zap.Error(err))
			// return nil to avoid infinite retry on DDL callback
			return nil
		}
		if err = WaitCollectionReleased(ctx, job.dist, job.checkerController, req.GetCollectionId(), toReleasePartitions...); err != nil {
			log.Warn("failed to wait partition released", zap.Error(err))
			// return nil to avoid infinite retry on DDL callback
			return nil
		}
		log.Info("wait for partition released done", zap.Int64s("toReleasePartitions", toReleasePartitions))
	}
	return nil
}

// releasePartitionsOnDelegators asks every QueryNode that hosts a delegator of the collection to
// release the partitions now. They have already left the meta, the next target and the load
// config, so nothing loads them again; but the segment checker releases their segments only once
// a delegator's readable view no longer holds them, and a delegator lagging behind the current
// target -- because it cannot load a newer segment, say -- keeps serving the released rows, and
// keeps their memory, until it catches up. The explicit call takes the partitions out of the
// delegator's view and distribution and releases the segments on their workers, so the results
// match the meta by the time this job returns. It is idempotent, as the DDL callback may run
// again. A node that cannot be reached is left to the wait that follows, which releases through
// the target sync as before.
func (job *LoadCollectionJob) releasePartitionsOnDelegators(ctx context.Context, collectionID int64, partitionIDs []int64) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", partitionIDs))
	if job.cluster == nil || job.dist == nil {
		log.Warn("no cluster to release partitions on, wait for the target sync to release them")
		return
	}
	delegators := job.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(collectionID))
	nodes := lo.Uniq(lo.Map(delegators, func(channel *meta.DmChannel, _ int) int64 { return channel.Node }))

	var wg sync.WaitGroup
	for _, nodeID := range nodes {
		wg.Add(1)
		go func(nodeID int64) {
			defer wg.Done()
			status, err := job.cluster.ReleasePartitions(ctx, nodeID, &querypb.ReleasePartitionsRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_ReleasePartitions),
					commonpbutil.WithTargetID(nodeID),
				),
				CollectionID: collectionID,
				PartitionIDs: partitionIDs,
				NodeID:       nodeID,
			})
			if err := merr.CheckRPCCall(status, err); err != nil {
				log.Warn("failed to release partitions on query node, wait for the target sync to release them",
					zap.Int64("nodeID", nodeID), zap.Error(err))
				return
			}
			log.Info("released partitions on query node", zap.Int64("nodeID", nodeID))
		}(nodeID)
	}
	wg.Wait()
}

// getLocalReplicaConfig reads the local cluster-level replica config and generates LoadReplicaConfig entries.
// It uses generateReplicas to ensure idempotency on WAL replay by reusing existing replicas from meta.
// If local config is not set, defaults to 1 replica in __default_resource_group.
func getLocalReplicaConfig(ctx context.Context, m *meta.Meta, collectionID int64) ([]*messagespb.LoadReplicaConfig, error) {
	replicaNum := int(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt64())
	rgs := paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()

	if replicaNum <= 0 {
		replicaNum = 1
	}
	if len(rgs) == 0 {
		rgs = []string{meta.DefaultResourceGroupName}
	}

	// Use AssignReplica to determine expected replica distribution per RG
	expectedReplicaNumber, err := utils.AssignReplica(ctx, m, rgs, int32(replicaNum), false)
	if err != nil {
		return nil, err
	}

	// Get current replicas from meta for idempotent generation
	currentReplicas := m.GetByCollection(ctx, collectionID)
	currentReplicaMap := make(map[int64]*meta.Replica)
	for _, r := range currentReplicas {
		currentReplicaMap[r.GetID()] = r
	}

	// Use generateReplicas which reuses existing replicas (idempotent on replay)
	req := &AlterLoadConfigRequest{
		Meta:     m,
		Current:  CurrentLoadConfig{Replicas: currentReplicaMap},
		Expected: ExpectedLoadConfig{ExpectedReplicaNumber: expectedReplicaNumber},
	}
	return req.generateReplicas(ctx)
}
