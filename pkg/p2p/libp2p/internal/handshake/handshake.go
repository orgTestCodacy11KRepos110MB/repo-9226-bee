// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handshake

import (
	"errors"
	"fmt"

	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/p2p"
	"github.com/ethersphere/bee/pkg/p2p/libp2p/internal/handshake/pb"
	"github.com/ethersphere/bee/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/pkg/swarm"
)

const (
	ProtocolName  = "handshake"
	StreamName    = "handshake"
	StreamVersion = "1.0.0"
)

// ErrNetworkIDIncompatible should be returned by handshake handlers if
// response from the other peer does not have valid networkID.
var ErrNetworkIDIncompatible = errors.New("incompatible networkID")

// ErrHandshakeDuplicate should be returned by handshake handlers if
// the handshake response has been received by an already processed peer.
var ErrHandshakeDuplicate = errors.New("duplicate handshake")

// PeerFinder has the information if the peer already exists in swarm.
type PeerFinder interface {
	Exists(overlay swarm.Address) (found bool)
}

type Service struct {
	peerFinder PeerFinder
	overlay    swarm.Address
	networkID  int32
	logger     logging.Logger
}

func New(peerFinder PeerFinder, overlay swarm.Address, networkID int32, logger logging.Logger) *Service {
	return &Service{
		peerFinder: peerFinder,
		overlay:    overlay,
		networkID:  networkID,
		logger:     logger,
	}
}

func (s *Service) Handshake(stream p2p.Stream) (i *Info, err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	var resp pb.SynAck
	if err := w.WriteMsg(&pb.Syn{
		Address:   s.overlay.Bytes(),
		NetworkID: s.networkID,
	}); err != nil {
		return nil, fmt.Errorf("write message: %w", err)
	}

	if err := r.ReadMsg(&resp); err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}

	address := swarm.NewAddress(resp.Syn.Address)
	if s.peerFinder.Exists(address) {
		return nil, ErrHandshakeDuplicate
	}

	if resp.Syn.NetworkID != s.networkID {
		return nil, ErrNetworkIDIncompatible
	}

	if err := w.WriteMsg(&pb.Ack{Address: resp.Syn.Address}); err != nil {
		return nil, fmt.Errorf("ack: write message: %w", err)
	}

	s.logger.Tracef("handshake finished for peer %s", address)

	return &Info{
		Address:   address,
		NetworkID: resp.Syn.NetworkID,
		Light:     resp.Syn.Light,
	}, nil
}

func (s *Service) Handle(stream p2p.Stream) (i *Info, err error) {
	w, r := protobuf.NewWriterAndReader(stream)
	defer stream.Close()

	var req pb.Syn
	if err := r.ReadMsg(&req); err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}

	address := swarm.NewAddress(req.Address)
	if s.peerFinder.Exists(address) {
		return nil, ErrHandshakeDuplicate
	}

	if req.NetworkID != s.networkID {
		return nil, ErrNetworkIDIncompatible
	}

	if err := w.WriteMsg(&pb.SynAck{
		Syn: &pb.Syn{
			Address:   s.overlay.Bytes(),
			NetworkID: s.networkID,
		},
		Ack: &pb.Ack{Address: req.Address},
	}); err != nil {
		return nil, fmt.Errorf("write message: %w", err)
	}

	var ack pb.Ack
	if err := r.ReadMsg(&ack); err != nil {
		return nil, fmt.Errorf("ack: read message: %w", err)
	}

	s.logger.Tracef("handshake finished for peer %s", address)
	return &Info{
		Address:   address,
		NetworkID: req.NetworkID,
		Light:     req.Light,
	}, nil
}

type Info struct {
	Address   swarm.Address
	NetworkID int32
	Light     bool
}
