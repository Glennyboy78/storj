// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo

import (
	"context"

	"go.uber.org/zap"
	monkit "gopkg.in/spacemonkeygo/monkit.v2"

	"storj.io/storj/pkg/pb"
)

var (
	mon = monkit.Package()
)

// Endpoint implements the network state RPC service
type Endpoint struct {
	log    *zap.Logger
	config Config
}

// NewEndpoint creates instance of Endpoint
func NewEndpoint(log *zap.Logger, config Config) *Endpoint {
	return &Endpoint{
		log:    log,
		config: config,
	}
}

// Close closes resources
func (e *Endpoint) Close() error { return nil }

// Health returns the health of a specific path
func (e *Endpoint) Health(ctx context.Context, req *pb.ObjectHealthRequest) (resp *pb.ObjectHealthResponse, err error) {
	defer mon.Task()(&ctx)(&err)

	resp = &pb.ObjectHealthResponse{}

	// Find segements by req.EncryptedPath, req.Bucket and req.UplinkId
	// for each segment
	// 		determine number of good nodes and bad nodes
	// 		append to Response

	return resp, nil
}