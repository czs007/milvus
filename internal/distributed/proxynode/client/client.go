package grpcproxynodeclient

import (
	"context"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"google.golang.org/grpc"
)

type Client struct {
	grpcClient proxypb.ProxyNodeServiceClient
	address    string
	ctx        context.Context
}

func (c *Client) Init() error {
	connectGrpcFunc := func() error {
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		c.grpcClient = proxypb.NewProxyNodeServiceClient(conn)
		return nil
	}
	err := retry.Retry(10, time.Millisecond*200, connectGrpcFunc)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return nil
}

func (c *Client) InvalidateCollectionMetaCache(request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return c.grpcClient.InvalidateCollectionMetaCache(c.ctx, request)
}

func NewClient(ctx context.Context, address string) *Client {
	return &Client{
		address: address,
		ctx:     ctx,
	}
}