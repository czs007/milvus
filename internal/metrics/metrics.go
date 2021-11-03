package metrics

import (
	"net/http"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
)

const (
	milvusNamespace     = "milvus"
	subSystemRootCoord  = "rootcoord"
	subSystemDataCoord  = "dataCoord"
	subSystemDataNode   = "dataNode"
	subSystemProxy      = "proxy"
	subSystemQueryNode  = "queryNode"
	subSystemQueryCoord = "queryCoord"
	subSystemIndexNode  = "indexNode"
	subSystemIndexCoord = "indexCoord"
)

var (
	// RootCoordProxyLister counts the num of registered proxy nodes
	RootCoordProxyLister = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "list_of_proxy",
			Help:      "List of proxy nodes which have registered with etcd",
		}, []string{"client_id"})

	////////////////////////////////////////////////////////////////////////////
	// for grpc

	// RootCoordCreateCollectionCounter counts the num of calls of CreateCollection
	RootCoordCreateCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "create_collection_total",
			Help:      "Counter of create collection",
		}, []string{"client_id", "type"})

	// RootCoordDropCollectionCounter counts the num of calls of DropCollection
	RootCoordDropCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "drop_collection_total",
			Help:      "Counter of drop collection",
		}, []string{"client_id", "type"})

	// czs
	// RootCoordHasCollectionCounter counts the num of calls of HasCollection
	RootCoordHasCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "has_collection_total",
			Help:      "Counter of has collection",
		}, []string{"client_id", "type"})

	// RootCoordDescribeCollectionCounter counts the num of calls of DescribeCollection
	RootCoordDescribeCollectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "describe_collection_total",
			Help:      "Counter of describe collection",
		}, []string{"client_id", "type"})

	// RootCoordShowCollectionsCounter counts the num of calls of ShowCollections
	RootCoordShowCollectionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "show_collections_total",
			Help:      "Counter of show collections",
		}, []string{"client_id", "type"})

	// RootCoordCreatePartitionCounter counts the num of calls of CreatePartition
	RootCoordCreatePartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "create_partition_total",
			Help:      "Counter of create partition",
		}, []string{"client_id", "type"})

	// RootCoordDropPartitionCounter counts the num of calls of DropPartition
	RootCoordDropPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "drop_partition_total",
			Help:      "Counter of drop partition",
		}, []string{"client_id", "type"})

	// RootCoordHasPartitionCounter counts the num of calls of HasPartition
	RootCoordHasPartitionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "has_partition_total",
			Help:      "Counter of has partition",
		}, []string{"client_id", "type"})

	// RootCoordShowPartitionsCounter counts the num of calls of ShowPartitions
	RootCoordShowPartitionsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "show_partitions_total",
			Help:      "Counter of show partitions",
		}, []string{"client_id", "type"})

	// RootCoordCreateIndexCounter counts the num of calls of CreateIndex
	RootCoordCreateIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "create_index_total",
			Help:      "Counter of create index",
		}, []string{"client_id", "type"})

	// RootCoordDropIndexCounter counts the num of calls of DropIndex
	RootCoordDropIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "drop_index_total",
			Help:      "Counter of drop index",
		}, []string{"client_id", "type"})

	// RootCoordDescribeIndexCounter counts the num of calls of DescribeIndex
	RootCoordDescribeIndexCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "describe_index_total",
			Help:      "Counter of describe index",
		}, []string{"client_id", "type"})

	// RootCoordDescribeSegmentCounter counts the num of calls of DescribeSegment
	RootCoordDescribeSegmentCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "describe_segment_total",
			Help:      "Counter of describe segment",
		}, []string{"client_id", "type"})

	// RootCoordShowSegmentsCounter counts the num of calls of ShowSegments
	RootCoordShowSegmentsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "show_segments_total",
			Help:      "Counter of show segments",
		}, []string{"client_id", "type"})

	////////////////////////////////////////////////////////////////////////////
	// for time tick

	// RootCoordInsertChannelTimeTick counts the time tick num of insert channel in 24H
	RootCoordInsertChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "insert_channel_time_tick",
			Help:      "Time tick of insert Channel in 24H",
		}, []string{"vchannel"})

	// RootCoordDDChannelTimeTick counts the time tick num of dd channel in 24H
	RootCoordDDChannelTimeTick = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemRootCoord,
			Name:      "dd_channel_time_tick",
			Help:      "Time tick of dd Channel in 24H",
		})
)

//RegisterRootCoord registers RootCoord metrics
func RegisterRootCoord() {
	prometheus.MustRegister(RootCoordProxyLister)

	// for grpc
	prometheus.MustRegister(RootCoordCreateCollectionCounter)
	prometheus.MustRegister(RootCoordDropCollectionCounter)
	prometheus.MustRegister(RootCoordHasCollectionCounter)
	prometheus.MustRegister(RootCoordDescribeCollectionCounter)
	prometheus.MustRegister(RootCoordShowCollectionsCounter)
	prometheus.MustRegister(RootCoordCreatePartitionCounter)
	prometheus.MustRegister(RootCoordDropPartitionCounter)
	prometheus.MustRegister(RootCoordHasPartitionCounter)
	prometheus.MustRegister(RootCoordShowPartitionsCounter)
	prometheus.MustRegister(RootCoordCreateIndexCounter)
	prometheus.MustRegister(RootCoordDropIndexCounter)
	prometheus.MustRegister(RootCoordDescribeIndexCounter)
	prometheus.MustRegister(RootCoordDescribeSegmentCounter)
	prometheus.MustRegister(RootCoordShowSegmentsCounter)

	// for time tick
	prometheus.MustRegister(RootCoordInsertChannelTimeTick)
	prometheus.MustRegister(RootCoordDDChannelTimeTick)
}

var (
	ProxyDDLCounterTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "ddl_total",
			Help:      "Counter of ddl operation",
		}, []string{"node_id", "status", "type"})

	ProxyDQLCounterTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "dql_total",
			Help:      "Counter of dql operation",
		}, []string{"node_id", "status", "type"})

	ProxyDMLCounterTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "dml_total",
			Help:      "Counter of dml operation",
		}, []string{"node_id", "status", "type"})


	// ProxyRegisterLinkCounter counts the num of calls of RegisterLink
	ProxyRegisterLinkCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "register_link_total",
			Help:      "Counter of register link",
		}, []string{"node_id", "status"})

	// ProxyGetComponentStatesCounter counts the num of calls of GetComponentStates
	ProxyGetComponentStatesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "get_component_states_total",
			Help:      "Counter of get component states",
		}, []string{"node_id", "status"})

	// ProxyGetStatisticsChannelCounter counts the num of calls of GetStatisticsChannel
	ProxyGetStatisticsChannelCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "get_statistics_channel_total",
			Help:      "Counter of get statistics channel",
		}, []string{"node_id", "status"})

	// ProxyInvalidateCollectionMetaCacheCounter counts the num of calls of InvalidateCollectionMetaCache
	ProxyInvalidateCollectionMetaCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "invalidate_collection_meta_cache_total",
			Help:      "Counter of invalidate collection meta cache",
		}, []string{"node_id", "status"})

	// ProxyGetDmlChannelCounter counts the num of calls of GetDmlChannel
	ProxyGetDmlChannelCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "get_dml_channel_total",
			Help:      "Counter of get dml channel",
		}, []string{"node_id", "status"})

	// ProxyGetDqlChannelCounter counts the num of calls of GetDqlChannel
	ProxyGetDqlChannelCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "get_dql_channel_total",
			Help:      "Counter of get dql channel",
		}, []string{"node_id", "status"})

	// ProxyReleaseDQLMessageStreamCounter counts the num of calls of ReleaseDQLMessageStream
	ProxyReleaseDQLMessageStreamCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "release_dql_message_stream_total",
			Help:      "Counter of release dql message stream",
		}, []string{"node_id", "status"})

	// ProxyDmlChannelTimeTick counts the time tick value of dml channels
	ProxyDmlChannelTimeTick = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemProxy,
			Name:      "dml_channels_time_tick",
			Help:      "Time tick of dml channels",
		}, []string{"node_id", "channel_name"})
)

//RegisterProxy registers Proxy metrics
func RegisterProxy() {
	prometheus.MustRegister(ProxyDDLCounterTotal)
	prometheus.MustRegister(ProxyDQLCounterTotal)
	prometheus.MustRegister(ProxyDMLCounterTotal)

	prometheus.MustRegister(ProxyRegisterLinkCounter)

	prometheus.MustRegister(ProxyGetComponentStatesCounter)
	prometheus.MustRegister(ProxyGetStatisticsChannelCounter)

	prometheus.MustRegister(ProxyInvalidateCollectionMetaCacheCounter)
	prometheus.MustRegister(ProxyGetDmlChannelCounter)
	prometheus.MustRegister(ProxyGetDqlChannelCounter)

	prometheus.MustRegister(ProxyReleaseDQLMessageStreamCounter)

	prometheus.MustRegister(ProxyDmlChannelTimeTick)
}

//RegisterQueryCoord register QueryCoord metrics
func RegisterQueryCoord() {

}

//RegisterQueryNode register QueryNode metrics
func RegisterQueryNode() {

}

var (
	//DataCoordDataNodeList records the num of regsitered data nodes
	DataCoordDataNodeList = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemDataCoord,
			Name:      "list_of_data_node",
			Help:      "List of data nodes registered within etcd",
		}, []string{"status"},
	)
)

//RegisterDataCoord register DataCoord metrics
func RegisterDataCoord() {
	prometheus.MustRegister(DataCoordDataNodeList)
}

var (
	// DataNodeFlushSegmentsCounter used to count the num of calls of FlushSegments
	DataNodeFlushSegmentsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemDataNode,
			Name:      "flush_segments_total",
			Help:      "Counter of flush segments",
		}, []string{"type"})

	// DataNodeWatchDmChannelsCounter used to count the num of calls of WatchDmChannels
	DataNodeWatchDmChannelsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subSystemDataNode,
			Name:      "watch_dm_channels_total",
			Help:      "Counter of watch dm channel",
		}, []string{"type"})
)

//RegisterDataNode register DataNode metrics
func RegisterDataNode() {
	prometheus.MustRegister(DataNodeFlushSegmentsCounter)
	prometheus.MustRegister(DataNodeWatchDmChannelsCounter)
}

//RegisterIndexCoord register IndexCoord metrics
func RegisterIndexCoord() {

}

//RegisterIndexNode register IndexNode metrics
func RegisterIndexNode() {

}

//RegisterMsgStreamCoord register MsgStreamCoord metrics
func RegisterMsgStreamCoord() {

}

//ServeHTTP serve prometheus http service
func ServeHTTP() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(":9091", nil); err != nil {
			log.Error("handle metrics failed", zap.Error(err))
		}
	}()
}
