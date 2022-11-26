package reserve

import (
	"context"
	"time"

	storage "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
)

type reserve struct {
	store      storage.Store
	baseAddr   swarm.Address
	chunkStore storage.ChunkStore
}

func New(store storage.Store, baseAddr swarm.Address, chunkStore storage.ChunkStore) *reserve {
	return &reserve{store: store, baseAddr: baseAddr, chunkStore: chunkStore}
}

func (r *reserve) Put(ctx context.Context, chunk swarm.Chunk) error {

	has, err := r.store.Has(&batchRadiusItem{
		po:        r.po(chunk.Address()),
		address:   chunk.Address(),
		batchID:   chunk.Stamp().BatchID(),
		timestamp: 0,
	})
	if err != nil {
		return nil
	}
	if has {
		return nil
	}

	t := time.Now().UnixNano()

	err = r.store.Put(&batchRadiusItem{
		po:        r.po(chunk.Address()),
		address:   chunk.Address(),
		batchID:   chunk.Stamp().BatchID(),
		timestamp: uint64(t),
	})
	if err != nil {
		return nil
	}

	err = r.store.Put(&chunkProximityItem{
		po:        r.po(chunk.Address()),
		address:   chunk.Address(),
		timestamp: uint64(t),
	})
	if err != nil {
		return nil
	}

	_, err = r.chunkStore.Put(ctx, chunk)
	if err != nil {
		return nil
	}

	return nil
}

func (r *reserve) po(addr swarm.Address) uint8 {
	return swarm.Proximity(r.baseAddr.Bytes(), addr.Bytes())
}

func (r *reserve) EvictBatch(ctx context.Context, batchID []byte) error {

	err := r.store.Iterate(storage.Query{
		Factory: func() storage.Item { return &batchRadiusItem{batchID: batchID} },
	}, func(res storage.Result) (bool, error) {
		batchRadius := res.Entry.(*batchRadiusItem)
		batchRadius.batchID = batchID

		err := r.store.Delete(batchRadius)
		if err != nil {
			return false, err
		}

		err = r.store.Delete(&chunkProximityItem{
			po:        batchRadius.po,
			address:   batchRadius.address,
			timestamp: batchRadius.timestamp,
		})
		if err != nil {
			return false, err
		}

		err = r.chunkStore.Delete(ctx, batchRadius.address)
		if err != nil {
			return false, err
		}

		return false, nil
	})

	return err
}
