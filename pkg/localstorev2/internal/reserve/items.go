package reserve

import (
	"encoding/binary"
	"fmt"

	storage "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
)

const batchRadiusItemSize = 1 + swarm.HashSize + swarm.HashSize + 8

type batchRadiusItem struct {
	Bin     uint8
	BatchID []byte
	Address swarm.Address
	BinID   uint64
}

func (b *batchRadiusItem) Namespace() string {
	return "batchRadius"
}

// bin/batchID/ChunkAddr
func (b *batchRadiusItem) ID() string {
	return fmt.Sprintf("%s/%s", batchBinToString(b.Bin, b.BatchID), b.Address)
}

func batchBinToString(bin uint8, batchID []byte) string {
	return fmt.Sprintf("%d/%s", bin, batchID)
}

func (b *batchRadiusItem) Clone() storage.Item {
	return nil
}

func (b *batchRadiusItem) Marshal() ([]byte, error) {

	buf := make([]byte, batchRadiusItemSize)

	i := 0

	buf[i] = b.Bin
	i += 1

	copy(buf[i:], b.BatchID)
	i += swarm.HashSize

	copy(buf[i:], b.Address.Bytes())
	i += swarm.HashSize

	binary.BigEndian.PutUint64(buf[i:], b.BinID)

	return buf, nil
}

func (b *batchRadiusItem) Unmarshal(buf []byte) error {

	i := 0
	b.Bin = buf[i]
	i += 1

	b.BatchID = buf[i : i+swarm.HashSize]
	i += swarm.HashSize

	b.Address = swarm.NewAddress(buf[i : i+swarm.HashSize])
	i += swarm.HashSize

	b.BinID = binary.BigEndian.Uint64(buf[i : i+8])

	return nil
}

const chunkBinItemSize = 1 + 8 + swarm.HashSize

type chunkBinItem struct {
	Bin     uint8
	BinID   uint64
	Address swarm.Address
}

func binIDToString(bin uint8, binID uint64) string {
	return fmt.Sprintf("%s/%d", binToString(bin), binID)
}

func binToString(bin uint8) string {
	return fmt.Sprintf("%d", bin)
}

func (c *chunkBinItem) Namespace() string {
	return "chunkBin"
}

// bin/binID
func (c *chunkBinItem) ID() string {
	return binIDToString(c.Bin, c.BinID)
}

func (b *chunkBinItem) Clone() storage.Item {
	return nil
}

func (b *chunkBinItem) Marshal() ([]byte, error) {

	buf := make([]byte, chunkBinItemSize)

	i := 0

	buf[i] = b.Bin
	i += 1

	binary.BigEndian.PutUint64(buf[i:], b.BinID)
	i += 8

	copy(buf[i:], b.Address.Bytes())
	i += swarm.HashSize

	return buf, nil
}

func (b *chunkBinItem) Unmarshal(buf []byte) error {

	i := 0
	b.Bin = buf[i]
	i += 1

	b.BinID = binary.BigEndian.Uint64(buf[i : i+8])
	i += 8

	b.Address = swarm.NewAddress(buf[i : i+swarm.HashSize])
	i += swarm.HashSize

	return nil
}

const binItemSize = 8

type binItem struct {
	PO    uint8
	BinID uint64
}

func (b *binItem) Namespace() string {
	return "binID"
}

func (c *binItem) ID() string {
	return fmt.Sprintf("%d", c.PO)
}

func (b *binItem) Clone() storage.Item {
	return nil
}

func (b *binItem) Marshal() ([]byte, error) {
	buf := make([]byte, binItemSize)
	binary.BigEndian.PutUint64(buf, b.BinID)
	return buf, nil
}

func (b *binItem) Unmarshal(buf []byte) error {
	b.BinID = binary.BigEndian.Uint64(buf)
	return nil
}
