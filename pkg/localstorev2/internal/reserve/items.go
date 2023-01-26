package reserve

import (
	"fmt"

	storage "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
)

const batchRadiusItemSize = 1 + swarm.HashSize + 8

type batchRadiusItem struct {
	batchID   []byte
	PO        uint8
	Address   swarm.Address
	Timestamp []byte
	BinID     uint64
}

func (b *batchRadiusItem) Namespace() string {
	return fmt.Sprintf("batchRadius/%d/%s", b.PO, b.batchID)
}

func (b *batchRadiusItem) ID() string {
	return b.Address.ByteString()
}

func (b *batchRadiusItem) Clone() storage.Item {
	return nil
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
	binID     uint64
	address   swarm.Address
	timestamp []byte
}

func (c *chunkProximityItem) Namespace() string {
	return fmt.Sprintf("chunkProximity/%d", c.po)
}

func (c *chunkProximityItem) ID() string {
	return fmt.Sprintf("%d", c.binID)
}

func (b *chunkProximityItem) Clone() storage.Item {
	return nil
}

func (b *chunkProximityItem) Marshal() ([]byte, error) {

	// marshall address
	// marshall timestamp

	return nil, nil
}

func (b *chunkProximityItem) Unmarshal(buf []byte) error {
	return nil
}

type binItem struct {
	po    uint8
	binID uint64
}

func (b *binItem) Namespace() string {
	return "binIndex"
}

func (c *binItem) ID() string {
	return fmt.Sprintf("%d", c.po)
}

func (b *binItem) Clone() storage.Item {
	return nil
}

func (b *binItem) Marshal() ([]byte, error) {

	// marshall index

	return nil, nil
}

func (b *binItem) Unmarshal(buf []byte) error {
	return nil
}
