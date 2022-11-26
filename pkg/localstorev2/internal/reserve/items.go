package reserve

import (
	"fmt"

	"github.com/ethersphere/bee/pkg/swarm"
)

const batchRadiusItemSize = 1 + swarm.HashSize + 8

type batchRadiusItem struct {
	batchID []byte

	PO        uint8
	Address   swarm.Address
	Timestamp []byte
}

func (b *batchRadiusItem) Namespace() string {
	return "batchRadius"
}

func (b *batchRadiusItem) ID() string {
	return fmt.Sprintf("%s/%d/%s", b.batchID, b.PO, b.Address.ByteString())
}

func (b *batchRadiusItem) Marshal() ([]byte, error) {

	buf := make([]byte, batchRadiusItemSize)

	i := 0

	buf[i] = b.PO
	i += 1

	copy(buf[i:], b.Address.Bytes())
	i += swarm.HashSize

	copy(buf[i:], b.Timestamp)

	return buf, nil
}

func (b *batchRadiusItem) Unmarshal(buf []byte) error {

	i := 0
	b.PO = buf[i]
	i += 1

	b.Address = swarm.NewAddress(buf[i : i+swarm.HashSize])
	i += swarm.HashSize

	copy(buf[i:], b.Timestamp)
	return nil
}

type chunkProximityItem struct {
	po        uint8
	address   swarm.Address
	timestamp []byte
}

func (c *chunkProximityItem) Namespace() string {
	return "chunkProximity"
}

func (c *chunkProximityItem) ID() string {
	return fmt.Sprintf("%d/%d/%s", c.po, c.timestamp, c.address.ByteString())
}

func (b *chunkProximityItem) Marshal() ([]byte, error) {
	return nil, nil
}

func (b *chunkProximityItem) Unmarshal(buf []byte) error {
	return nil
}
