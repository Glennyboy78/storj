// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/net/context"

	"storj.io/storj/pkg/pb"
	"storj.io/storj/pkg/transport"
)

var (
	// ClientError wraps errors returned from client package
	ClientError = errs.Class("metainfo client error")
)

// Client maintains variables required for talking to basic satellite endpoints
type Client struct {
	log       *zap.Logger
	transport transport.Client
	satellite string
}

// New creates a Client
func New(log *zap.Logger, transport transport.Client, satellite string) *Client {
	return &Client{log: log, transport: transport, satellite: satellite}
}

// Stat will return the health of a specific path
func (client *Client) Stat(ctx context.Context, path []byte, bucket []byte) (*pb.ObjectHealthResponse, error) {
	// Create client from satellite ip
	conn, err := client.transport.DialAddress(ctx, client.satellite)
	if err != nil {
		return nil, ClientError.Wrap(err)
	}

	metainfoClient := pb.NewMetainfoClient(conn)
	defer func() {
		err := conn.Close()
		if err != nil {
			return err
		}
	}()

	req := &pb.ObjectHealthRequest{
		EncryptedPath: path,
		Bucket:        bucket,
		UplinkId:      client.transport.Identity().ID,
	}

	return metainfoClient.Health(ctx, req)
}