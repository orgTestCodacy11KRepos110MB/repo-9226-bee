package reserve

import (
	"fmt"

	storage "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
)

const batchRadiusItemSize = 1 + swarm.HashSize + 8

type batchRadiusRoot struct{}

func (b *batchRadiusRoot) Namespace() string { return "batchRadius" }
func (b *batchRadiusRoot) ID() string        { return "" }

type batchRadiusItem struct {
	batchID []byte
	Bin     uint8
	Address swarm.Address
	BinID   uint64
}

func (b *batchRadiusItem) Namespace() string {
	return fmt.Sprintf("batchRadius/%d/%s", b.Bin, b.batchID)
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

	buf[i] = b.Bin
	i += 1

	copy(buf[i:], b.Address.Bytes())
	i += swarm.HashSize

	return buf, nil
}

func (b *batchRadiusItem) Unmarshal(buf []byte) error {

	i := 0
	b.Bin = buf[i]
	i += 1

	b.Address = swarm.NewAddress(buf[i : i+swarm.HashSize])
	i += swarm.HashSize

	return nil
}

type chunkBinItem struct {
	bin     uint8
	binID   uint64
	address swarm.Address
}

func binIDToString(binID uint64) string {
	return fmt.Sprintf("%d", binID)
}

func (c *chunkBinItem) Namespace() string {
	return fmt.Sprintf("chunkBin/%d", c.bin)
}

func (c *chunkBinItem) ID() string {
	return binIDToString(c.binID)
}

func (b *chunkBinItem) Clone() storage.Item {
	return nil
}

func (b *chunkBinItem) Marshal() ([]byte, error) {

	// marshall address
	// marshall timestamp

	return nil, nil
}

func (b *chunkBinItem) Unmarshal(buf []byte) error {
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
	return nil, nil
}

func (b *binItem) Unmarshal(buf []byte) error {
	return nil
}
