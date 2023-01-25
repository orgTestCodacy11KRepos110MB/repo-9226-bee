package reserve

import (
	"context"
	"errors"

	storage "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
)

type reserve struct {
	store      storage.Store
	baseAddr   swarm.Address
	chunkStore storage.ChunkStore
	count      int
	capacity   int
}

/*
	pull by po - binID
	evict by po - batch
	sample by po
*/

func New(store storage.Store, baseAddr swarm.Address, chunkStore storage.ChunkStore, capacity int) (*reserve, error) {

	size, err := store.Count(&batchRadiusItem{})
	if err != nil {
		return nil, err
	}

	rs := &reserve{store, baseAddr, chunkStore, size, capacity}

	rs.limitSize()

	return rs, nil
}

func (r *reserve) Put(ctx context.Context, chunk swarm.Chunk) error {

	has, err := r.store.Has(&batchRadiusItem{
		PO:      r.po(chunk.Address()),
		Address: chunk.Address(),
		batchID: chunk.Stamp().BatchID(),
	})
	if err != nil {
		return nil
	}
	if has {
		return nil
	}

	po := r.po(chunk.Address())
	binID, err := r.incBinID(po)
	if err != nil {
		return nil
	}

	err = r.store.Put(&batchRadiusItem{
		PO:        po,
		Address:   chunk.Address(),
		batchID:   chunk.Stamp().BatchID(),
		Timestamp: chunk.Stamp().Timestamp(),
		BinID:     binID,
	})
	if err != nil {
		return nil
	}

	defer func() {
		r.count++
		r.limitSize()
	}()

	err = r.store.Put(&chunkProximityItem{
		po:        po,
		binID:     binID,
		address:   chunk.Address(),
		timestamp: chunk.Stamp().Timestamp(),
	})
	if err != nil {
		return nil
	}

	return r.chunkStore.Put(ctx, chunk)
}

func (r *reserve) po(addr swarm.Address) uint8 {
	return swarm.Proximity(r.baseAddr.Bytes(), addr.Bytes())
}

func (r *reserve) incBinID(po uint8) (uint64, error) {

	bin := &binItem{po: po}
	err := r.store.Get(bin)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, r.store.Put(bin)
		}

		return 0, err
	}

	bin.binID += 1

	return bin.binID, r.store.Put(bin)
}

func (r *reserve) limitSize() {
	if r.count > r.capacity {
		// TODO: ask batchstore to call UnreserveBatch
	}
}

// TODO: reserve sampler
func (r *reserve) Sample(po uint8) error {
	return nil
}

func (r *reserve) UnreserveBatch(ctx context.Context, batchID []byte, po uint8) (bool, error) {

	for i := uint8(0); i < po; i++ {
		err := r.store.Iterate(storage.Query{
			Factory: func() storage.Item {
				return &batchRadiusItem{
					batchID: batchID,
					PO:      po,
				}
			},
		}, func(res storage.Result) (bool, error) {
			batchRadius := res.Entry.(*batchRadiusItem)
			batchRadius.batchID = batchID

			err := r.store.Delete(batchRadius)
			if err != nil {
				return false, err
			}

			r.count--

			err = r.store.Delete(&chunkProximityItem{
				po:        batchRadius.PO,
				address:   batchRadius.Address,
				timestamp: batchRadius.Timestamp,
				binID:     batchRadius.BinID,
			})
			if err != nil {
				return false, err
			}

			err = r.chunkStore.Delete(ctx, batchRadius.Address)
			if err != nil {
				return false, err
			}

			return false, nil
		})

		if err != nil {
			return r.count <= r.capacity, err
		}
	}

	return r.count <= r.capacity, nil
}
