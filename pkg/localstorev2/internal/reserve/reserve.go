package reserve

import (
	"context"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethersphere/bee/pkg/bmtpool"
	"github.com/ethersphere/bee/pkg/log"
	"github.com/ethersphere/bee/pkg/postage"
	"github.com/ethersphere/bee/pkg/puller"
	"github.com/ethersphere/bee/pkg/shed"
	storage "github.com/ethersphere/bee/pkg/storagev2"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/topology"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
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

const sampleSize = 8

var errDbClosed = errors.New("database closed")
var errSamplerStopped = errors.New("sampler stopped due to ongoing evictions")

type sampleStat struct {
	TotalIterated      atomic.Int64
	NotFound           atomic.Int64
	NewIgnored         atomic.Int64
	IterationDuration  atomic.Int64
	GetDuration        atomic.Int64
	HmacrDuration      atomic.Int64
	ValidStampDuration atomic.Int64
}

type sampleEntry struct {
	transformedAddress swarm.Address
	chunkItem          shed.Item
}

func (s sampleStat) String() string {

	seconds := int64(time.Second)

	return fmt.Sprintf(
		"Chunks: %d NotFound: %d New Ignored: %d Iteration Duration: %d secs GetDuration: %d secs"+
			" HmacrDuration: %d secs ValidStampDuration: %d secs",
		s.TotalIterated.Load(),
		s.NotFound.Load(),
		s.NewIgnored.Load(),
		s.IterationDuration.Load()/seconds,
		s.GetDuration.Load()/seconds,
		s.HmacrDuration.Load()/seconds,
		s.ValidStampDuration.Load()/seconds,
	)
}

// TODO
func (r *reserve) Sample(ctx context.Context,
	anchor []byte,
	storageRadius uint8,
	consensusTime uint64,
) (Sample, error) {

	g, ctx := errgroup.WithContext(ctx)

	addrChan := make(chan swarm.Address)
	var stat sampleStat
	logger := r.logger.WithName("sampler").V(1).Register()

	t := time.Now()

	// Phase 1: Iterate chunk addresses
	g.Go(func() error {
		defer close(addrChan)
		iterationStart := time.Now()

		err := r.store.Iterate(storage.Query{
			Factory: func() storage.Item {
				return &chunkBinItem{
					bin: storageRadius,
				}
			},
			Prefix: binToString(storageRadius),
		}, func(res storage.Result) (bool, error) {

			item := res.Entry.(*chunkBinItem)

			select {
			case addrChan <- item.address:
				stat.TotalIterated.Inc()
				return false, nil
			case <-ctx.Done():
				return true, ctx.Err()
			}

			return false, nil
		})
		if err != nil {
			return err
		}

		stat.IterationDuration.Add(time.Since(iterationStart).Nanoseconds())
		return nil
	})

	// Phase 2: Get the chunk data and calculate transformed hash
	sampleItemChan := make(chan sampleEntry)
	const workers = 6
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			hmacr := hmac.New(swarm.NewHasher, anchor)

			for addr := range addrChan {
				getStart := time.Now()
				chItem, err := db.get(ctx, storage.ModeGetSync, addr)
				stat.GetDuration.Add(time.Since(getStart).Nanoseconds())
				if err != nil {
					stat.NotFound.Inc()
					continue
				}

				// check if the timestamp on the postage stamp is not later than
				// the consensus time.
				if binary.BigEndian.Uint64(chItem.Timestamp) > consensusTime {
					stat.NewIgnored.Inc()
					continue
				}

				hmacrStart := time.Now()
				_, err = hmacr.Write(chItem.Data)
				if err != nil {
					return err
				}
				taddr := hmacr.Sum(nil)
				hmacr.Reset()
				stat.HmacrDuration.Add(time.Since(hmacrStart).Nanoseconds())

				select {
				case sampleItemChan <- sampleEntry{transformedAddress: swarm.NewAddress(taddr), chunkItem: chItem}:
					// continue
				case <-ctx.Done():
					return ctx.Err()
				case <-db.close:
					return errDbClosed
				case <-db.samplerSignal:
					return errSamplerStopped
				}
			}

			return nil
		})
	}

	go func() {
		_ = g.Wait()
		close(sampleItemChan)
	}()

	sampleItems := make([]swarm.Address, 0, sampleSize)
	// insert function will insert the new item in its correct place. If the sample
	// size goes beyond what we need we omit the last item.
	insert := func(item swarm.Address) {
		added := false
		for i, sItem := range sampleItems {
			if le(item.Bytes(), sItem.Bytes()) {
				sampleItems = append(sampleItems[:i+1], sampleItems[i:]...)
				sampleItems[i] = item
				added = true
				break
			}
		}
		if len(sampleItems) > sampleSize {
			sampleItems = sampleItems[:sampleSize]
		}
		if len(sampleItems) < sampleSize && !added {
			sampleItems = append(sampleItems, item)
		}
	}

	// Phase 3: Assemble the sample. Here we need to assemble only the first sampleSize
	// no of items from the results of the 2nd phase.
	for item := range sampleItemChan {
		var currentMaxAddr swarm.Address
		if len(sampleItems) > 0 {
			currentMaxAddr = sampleItems[len(sampleItems)-1]
		} else {
			currentMaxAddr = swarm.NewAddress(make([]byte, 32))
		}
		if le(item.transformedAddress.Bytes(), currentMaxAddr.Bytes()) || len(sampleItems) < sampleSize {

			validStart := time.Now()

			chunk := swarm.NewChunk(swarm.NewAddress(item.chunkItem.Address), item.chunkItem.Data)

			stamp := postage.NewStamp(
				item.chunkItem.BatchID,
				item.chunkItem.Index,
				item.chunkItem.Timestamp,
				item.chunkItem.Sig,
			)

			stampData, err := stamp.MarshalBinary()
			if err != nil {
				logger.Debug("error marshaling stamp for chunk", "chunk_address", chunk.Address(), "error", err)
				continue
			}
			_, err = db.validStamp(chunk, stampData)
			if err == nil {
				if !validChunkFn(chunk) {
					logger.Debug("data invalid for chunk address", "chunk_address", chunk.Address())
				} else {
					insert(item.transformedAddress)
				}
			} else {
				logger.Debug("invalid stamp for chunk", "chunk_address", chunk.Address(), "error", err)
			}

			stat.ValidStampDuration.Add(time.Since(validStart).Nanoseconds())
		}
	}

	if err := g.Wait(); err != nil {
		db.metrics.SamplerFailedRuns.Inc()
		if errors.Is(err, errSamplerStopped) {
			db.metrics.SamplerStopped.Inc()
		}
		return storage.Sample{}, fmt.Errorf("sampler: failed creating sample: %w", err)
	}

	hasher := bmtpool.Get()
	defer bmtpool.Put(hasher)

	for _, s := range sampleItems {
		_, err := hasher.Write(s.Bytes())
		if err != nil {
			db.metrics.SamplerFailedRuns.Inc()
			return storage.Sample{}, fmt.Errorf("sampler: failed creating root hash of sample: %w", err)
		}
	}
	hash := hasher.Sum(nil)

	sample := storage.Sample{
		Items: sampleItems,
		Hash:  swarm.NewAddress(hash),
	}

	db.metrics.SamplerSuccessfulRuns.Inc()
	logger.Info("sampler done", "duration", time.Since(t), "storage_radius", storageRadius, "consensus_time_ns", consensusTime, "stats", stat, "sample", sample)

	return sample, nil

	return Sample{}, nil
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
