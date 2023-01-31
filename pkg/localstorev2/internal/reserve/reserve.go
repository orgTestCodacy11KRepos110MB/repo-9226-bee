package reserve

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/puller"
	"github.com/ethersphere/bee/pkg/storage"
	storagev2 "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/topology"
)

const storageRadiusKey = "reserve_storage_radius"

type reserve struct {
	mtx sync.Mutex

	store      storagev2.Store
	baseAddr   swarm.Address
	chunkStore storagev2.ChunkStore
	size       int
	capacity   int
	subscriber *binSubscriber

	radius uint8
	// TODO
	bs           postage.Storer
	radiusSetter topology.SetStorageRadiuser
	syncer       puller.SyncReporter
	logger       log.Logger
	stateStore   storage.StateStorer

	quit chan struct{}
	wg   sync.WaitGroup
}

type Sample struct {
	Items []swarm.Address
	Hash  swarm.Address
}

/*
	pull by 	bin - binID
	evict by 	bin - batchID
	sample by 	bin


	GOALS:
	1. The batchstore should just contain the list of batches, and calculate the reserve radius, and calls UnreserveBatch when the batch expires
	2. The new reserve will maintain the storage radius and the radius notifier (to kademlia)
	3. When reserve is full, it will iterate on the list of batches from the batchstore, then at the end calls bs.SetRadius()

*/

func New(store storagev2.Store, stateStore storage.StateStorer, baseAddr swarm.Address, chunkStore storagev2.ChunkStore, capacity int) (*reserve, error) {

	size, err := store.Count(&batchRadiusItem{})
	if err != nil {
		return nil, err
	}

	var radius uint8
	err = stateStore.Get(storageRadiusKey, &radius)
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
		quit:       make(chan struct{}),
		radius:     radius,
	}

	rs.wg.Add(1)
	go rs.radiusManager()

	return rs, nil
}

func (r *reserve) radiusManager() {

	defer r.wg.Done()

	for {
		select {
		case <-time.After(time.Minute * 5):
		default:
		}

		r.mtx.Lock()

		if r.size >= (r.capacity * 4 / 10) {
			continue
		}

		if r.syncer.Rate() == 0 {
			r.radius--
			r.radiusSetter.SetStorageRadius(r.radius)
			_ = r.stateStore.Put(storageRadiusKey, &r.radius)
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
		BatchID: chunk.Stamp().BatchID(),
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
		BatchID: chunk.Stamp().BatchID(),
		BinID:   binID,
	})
	if err != nil {
		return nil
	}

	err = r.store.Put(&chunkBinItem{
		Bin:     bin,
		BinID:   binID,
		Address: chunk.Address(),
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
func (r *reserve) unreserveBatchBin(ctx context.Context, batchID []byte, bin uint8) error {

	for i := uint8(0); i < bin; i++ {
		err := r.store.Iterate(storagev2.Query{
			Factory: func() storagev2.Item {
				return &batchRadiusItem{}
			},
			Prefix: batchBinToString(bin, batchID),
		}, func(res storagev2.Result) (bool, error) {

			batchRadius := res.Entry.(*batchRadiusItem)

			err := r.store.Delete(batchRadius)
			if err != nil {
				return false, err
			}

			r.size--

			err = r.store.Delete(&chunkBinItem{
				Bin:   batchRadius.Bin,
				BinID: batchRadius.BinID,
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

type BinResult struct {
	Address swarm.Address
	BinID   uint64
	Error   error
}

func (r *reserve) SubscribeBin(ctx context.Context, bin uint8, start, end uint64) <-chan BinResult {

	out := make(chan BinResult, 1)

	sendError := func(err error) {
		select {
		case out <- BinResult{Error: err}:
		case <-ctx.Done():
		}
	}

	go func() {
		trigger, unsub := r.subscriber.Subscribe(bin)
		defer unsub()

		var lastBinID uint64

		for {
			err := r.store.Iterate(storagev2.Query{
				Factory: func() storagev2.Item {
					return &chunkBinItem{}
				},
				StartPrefix: binIDToString(bin, start),
			}, func(res storagev2.Result) (bool, error) {

				item := res.Entry.(*chunkBinItem)

				select {
				case out <- BinResult{Address: item.Address, BinID: item.BinID}:
				case <-ctx.Done():
					return false, ctx.Err()
				}

				lastBinID = item.BinID

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

func (r *reserve) Close() error {
	close(r.quit)
	r.wg.Wait()
	return nil
}

func (r *reserve) po(addr swarm.Address) uint8 {
	return swarm.Proximity(r.baseAddr.Bytes(), addr.Bytes())
}

// Must be called under lock.
func (r *reserve) incBinID(po uint8) (uint64, error) {

	bin := &binItem{PO: po}
	err := r.store.Get(bin)
	if err != nil {
		if errors.Is(err, storagev2.ErrNotFound) {
			return 0, r.store.Put(bin)
		}

		return 0, err
	}

	bin.BinID += 1

	return bin.BinID, r.store.Put(bin)
}

// Must be called under lock.
func (r *reserve) incSize(ctx context.Context) error {

	r.size++

	if r.size <= r.capacity {
		return nil
	}

	r.wg.Add(1)
	go func() {
		r.mtx.Lock()
		defer r.mtx.Unlock()
		defer r.wg.Done()

		for r.size > r.capacity {

			stopEarly := false

			err := r.bs.Iterate(func(b *postage.Batch) (bool, error) {
				err := r.unreserveBatchBin(ctx, b.ID, r.radius)
				if err != nil {
					return false, err
				}

				if r.size <= r.capacity {
					stopEarly = true
					return true, nil
				}

				return false, nil
			})
			if err != nil {
				return
			}
			if stopEarly {
				return
			}
			r.radius++
			r.radiusSetter.SetStorageRadius(r.radius)
			_ = r.stateStore.Put(storageRadiusKey, &r.radius)
		}
	}()

	return nil

}
