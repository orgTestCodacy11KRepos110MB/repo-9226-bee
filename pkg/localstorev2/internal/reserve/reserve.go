package reserve

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/puller"
	storage "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/topology"
)

type reserve struct {
	mtx sync.Mutex

	store      storage.Store
	baseAddr   swarm.Address
	chunkStore storage.ChunkStore
	size       int
	capacity   int
	subscriber *binSubscriber

	// TODO
	bs           postage.Storer
	radius       uint8
	radiusSetter topology.SetStorageRadiuser
	syncer       puller.SyncReporter
	logger       log.Logger
}

type Sample struct {
	Items []swarm.Address
	Hash  swarm.Address
}

/*
	pull by 	po - binID
	evict by 	po - batchID
	sample by 	po


	GOALS:
	1. The batchstore should just contain the list of batches, and calculate the reserve radius, and calls UnreserveBatch when the batch expires
	2. The new reserve will maintain the storage radius and the radius notifier (to kademlia)
	3. When reserve is full, it will iterate on the list of batches from the batchstore, then at the end calls bs.SetRadius()

*/

func New(store storage.Store, baseAddr swarm.Address, chunkStore storage.ChunkStore, capacity int) (*reserve, error) {

	size, err := store.Count(&batchRadiusItem{})
	if err != nil {
		return nil, err
	}

	rs := &reserve{
		store:      store,
		baseAddr:   baseAddr,
		chunkStore: chunkStore,
		size:       size,
		capacity:   capacity,
		subscriber: newBinSubscriber(),
	}

	go rs.radiusManager()

	return rs, nil
}

func (r *reserve) radiusManager() {

	for {
		select {
		case <-time.After(time.Minute * 5):
		default:
		}

		r.mtx.Lock()

		if r.size > (r.capacity * 4 / 10) {
			continue
		}

		if r.syncer.Rate() == 0 {
			r.radius--
			r.radiusSetter.SetStorageRadius(r.radius)
		}

		r.mtx.Unlock()
	}
}

// TODO: put should check that the po is within radius
func (r *reserve) Put(ctx context.Context, chunk swarm.Chunk) error {

	r.mtx.Lock()
	defer r.mtx.Unlock()

	has, err := r.store.Has(&batchRadiusItem{
		Bin:     r.po(chunk.Address()),
		Address: chunk.Address(),
		batchID: chunk.Stamp().BatchID(),
	})
	if err != nil {
		return nil
	}
	if has {
		return nil
	}

	bin := r.po(chunk.Address())
	binID, err := r.incBinID(bin)
	if err != nil {
		return nil
	}

	err = r.store.Put(&batchRadiusItem{
		Bin:     bin,
		Address: chunk.Address(),
		batchID: chunk.Stamp().BatchID(),
		BinID:   binID,
	})
	if err != nil {
		return nil
	}

	err = r.store.Put(&chunkBinItem{
		bin:     bin,
		binID:   binID,
		address: chunk.Address(),
	})
	if err != nil {
		return nil
	}

	err = r.chunkStore.Put(ctx, chunk)
	if err != nil {
		return nil
	}

	r.subscriber.Trigger(bin)

	r.incSize(ctx)

	return nil
}

func (r *reserve) EvictBatch(ctx context.Context, batchID []byte) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.unreserveBatchBin(ctx, batchID, swarm.MaxBins)
}

// Must be called under lock.
func (r *reserve) unreserveBatchBin(ctx context.Context, batchID []byte, po uint8) error {

	for i := uint8(0); i < po; i++ {
		err := r.store.Iterate(storage.Query{
			Factory: func() storage.Item {
				return &batchRadiusItem{}
			},
			Prefix: batchBinToString(po, batchID),
		}, func(res storage.Result) (bool, error) {

			batchRadius := res.Entry.(*batchRadiusItem)
			batchRadius.batchID = batchID
			batchRadius.Bin = po

			err := r.store.Delete(batchRadius)
			if err != nil {
				return false, err
			}

			r.size--

			err = r.store.Delete(&chunkBinItem{
				bin:   batchRadius.Bin,
				binID: batchRadius.BinID,
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
			return err
		}
	}

	return nil
}

// less function uses the byte compare to check for lexicographic ordering
func le(a, b []byte) bool {
	return bytes.Compare(a, b) == -1
}

type BinResult struct {
	Address swarm.Address
	BinID   uint64
	Error   error
}

func (r *reserve) SubscribeBin(ctx context.Context, bin uint8, start, end uint64) <-chan BinResult {

	var (
		out       = make(chan BinResult, 1)
		lastBinID uint64
	)

	sendError := func(err error) {
		select {
		case out <- BinResult{Error: err}:
		case <-ctx.Done():
		}
	}

	trigger, unsub := r.subscriber.Subscribe(bin)
	defer unsub()

	go func() {
		for {
			err := r.store.Iterate(storage.Query{
				Factory: func() storage.Item {
					return &chunkBinItem{}
				},
				Prefix: binIDToString(bin, start),
			}, func(res storage.Result) (bool, error) {

				item := res.Entry.(*chunkBinItem)

				select {
				case out <- BinResult{Address: item.address, BinID: item.binID}:
				case <-ctx.Done():
					return false, ctx.Err()
				}

				lastBinID = item.binID

				if lastBinID == end {
					return true, nil
				}

				return false, nil
			})
			if err != nil {
				sendError(ctx.Err())
				return
			}

			if lastBinID == end {
				return
			}

			start = lastBinID + 1

			select {
			case <-trigger:
			case <-ctx.Done():
				sendError(ctx.Err())
			}
		}
	}()

	return out

}

func (r *reserve) po(addr swarm.Address) uint8 {
	return swarm.Proximity(r.baseAddr.Bytes(), addr.Bytes())
}

// Must be called under lock.
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

func (r *reserve) incSize(ctx context.Context) error {

	r.size++

	if r.size < r.capacity {
		return nil
	}

	err := r.bs.Iterate(func(b *postage.Batch) (bool, error) {
		err := r.unreserveBatchBin(ctx, b.ID, r.radius)
		if err != nil {
			return false, err
		}

		if r.size < r.capacity {
			return true, nil
		}

		return false, nil
	})
	return err
}
