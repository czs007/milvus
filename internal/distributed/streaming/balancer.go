package streaming

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type balancerImpl struct {
	*walAccesserImpl
}

// GetWALDistribution returns the wal distribution of the streaming node.
func (b balancerImpl) ListStreamingNode(ctx context.Context) ([]types.StreamingNodeInfo, error) {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, nil
	}

	nodes, err := snmanager.StaticStreamingNodeManager.GetBalancer().GetAllStreamingNodes(ctx)
	if err != nil {
		return nil, err
	}
	nodeInfos := make([]types.StreamingNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		nodeInfos = append(nodeInfos, *node)
	}
	return nodeInfos, nil
}

// GetWALDistribution returns the wal distribution of the streaming node.
func (b balancerImpl) GetWALDistribution(ctx context.Context, nodeID int64) (*types.StreamingNodeAssignment, error) {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, nil
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	var result *types.StreamingNodeAssignment
	stopErr := errors.New("stop watching")
	err = sbalancer.WatchChannelAssignments(ctx, func(param balancer.WatchChannelAssignmentsCallbackParam) error {
		for _, assignment := range param.Relations {
			if assignment.Node.ServerID == nodeID {
				if result == nil {
					result = &types.StreamingNodeAssignment{
						NodeInfo: assignment.Node,
						Channels: make(map[string]types.PChannelInfo),
					}
				}
				result.Channels[assignment.Channel.Name] = assignment.Channel
			}
		}
		return errors.New("stop watching")
	})
	if errors.Is(err, stopErr) {
		if result == nil {
			return nil, merr.ErrNodeNotFound
		}
		return result, nil
	}
	return nil, err
}

// GetFrozenNodeIDs returns the frozen node ids.
func (b balancerImpl) GetFrozenNodeIDs(ctx context.Context) ([]int64, error) {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, nil
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	resp, err := sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config:     &streamingpb.WALBalancePolicyConfig{},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return nil, err
	}
	return resp.FreezeNodeIds, nil
}

// IsRebalanceSuspended returns whether the rebalance of the wal is suspended.
func (b balancerImpl) IsRebalanceSuspended(ctx context.Context) (bool, error) {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return false, err
	}
	if !ready {
		return false, nil
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	resp, err := sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config:     &streamingpb.WALBalancePolicyConfig{},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return false, err
	}
	return !resp.GetConfig().GetAllowRebalance(), nil
}

func (b balancerImpl) SuspendRebalance(ctx context.Context) error {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	_, err = sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: false,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance},
		},
	})
	return err
}

func (b balancerImpl) ResumeRebalance(ctx context.Context) error {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	_, err = sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{
			AllowRebalance: true,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{types.UpdateMaskPathWALBalancePolicyAllowRebalance},
		},
	})
	return err
}

func (b balancerImpl) FreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	_, err = sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			FreezeNodeIds: nodeIDs,
		},
	})
	return err
}

func (b balancerImpl) DefreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	ready, err := b.checkIfStreamingServiceReady(ctx)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}

	sbalancer := snmanager.StaticStreamingNodeManager.GetBalancer()
	_, err = sbalancer.UpdateBalancePolicy(ctx, &types.UpdateWALBalancePolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			DefreezeNodeIds: nodeIDs,
		},
	})
	return err
}

func (b balancerImpl) checkIfStreamingServiceReady(ctx context.Context) (bool, error) {
	if !paramtable.IsLocalComponentEnabled(typeutil.MixCoordRole) {
		panic("should be only called at mix coord")
	}
	if err := snmanager.StaticStreamingNodeManager.CheckIfStreamingServiceReady(ctx); err != nil {
		if errors.Is(err, snmanager.ErrStreamingServiceNotReady) {
			// for 2.5.x compatibility, return empty result when streaming service is not ready.
			return false, nil
		}
		return false, err
	}
	return true, nil
}
