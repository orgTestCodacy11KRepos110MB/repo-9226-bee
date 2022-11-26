package reserve

import (
	"encoding/binary"
	"fmt"

	"github.com/ethersphere/bee/pkg/swarm"
)

const batchRadiusItemSize = 1 + swarm.HashSize + 8

type batchRadiusItem struct {
	po        uint8
	batchID   []byte
	address   swarm.Address
	timestamp uint64
}

func (b *batchRadiusItem) Namespace() string {
	return fmt.Sprintf("batchRadius/%s", b.batchID)
}

func (b *batchRadiusItem) ID() string {
	return fmt.Sprintf("%d/%s", b.po, b.address.ByteString())
}

func (b *batchRadiusItem) Marshal() ([]byte, error) {

	buf := make([]byte, batchRadiusItemSize)

	i := 0

	buf[i] = b.po
	i += 1

	copy(buf[i:], b.address.Bytes())
	i += swarm.HashSize

	binary.BigEndian.PutUint64(buf[i:], b.timestamp)

	return buf, nil
}

func (b *batchRadiusItem) Unmarshal(buf []byte) error {

	i := 0
	b.po = buf[i]
	i += 1

	b.address = swarm.NewAddress(buf[i : i+swarm.HashSize])
	i += swarm.HashSize

	b.timestamp = binary.BigEndian.Uint64(buf[i:])
	return nil
}

type chunkProximityItem struct {
	po        uint8
	address   swarm.Address
	timestamp uint64
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
