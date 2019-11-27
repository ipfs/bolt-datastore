package boltds

import (
	bolt "github.com/boltdb/bolt"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"
)

// BoltDatastore implements ds.Datastore
// TODO: use buckets to represent the hierarchy of the ds.Keys
type BoltDatastore struct {
	db         *bolt.DB
	bucketName []byte
	Path       string
}

func NewBoltDatastore(path, bucket string, noSync bool) (*BoltDatastore, error) {
	db, err := bolt.Open(path+"/bolt.db", 0600, nil)
	if err != nil {
		return nil, err
	}
	db.NoSync = noSync

	// TODO: need to do db.Close() sometime...
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &BoltDatastore{
		db:         db,
		bucketName: []byte(bucket),
		Path:       path + "/bolt.db",
	}, nil
}

func (bd *BoltDatastore) Close() error {
	return bd.db.Close()
}

func (bd *BoltDatastore) Delete(key ds.Key) error {
	return bd.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bd.bucketName).Delete(key.Bytes())
	})
}

func (bd *BoltDatastore) Get(key ds.Key) ([]byte, error) {
	var out []byte
	err := bd.db.View(func(tx *bolt.Tx) error {
		mmval := tx.Bucket(bd.bucketName).Get(key.Bytes())
		if mmval == nil {
			return ds.ErrNotFound
		}
		out = make([]byte, len(mmval))
		copy(out, mmval)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, err
}

func (bd *BoltDatastore) ConsumeValue(key ds.Key, f func([]byte) error) error {
	return bd.db.View(func(tx *bolt.Tx) error {
		mmval := tx.Bucket(bd.bucketName).Get(key.Bytes())
		if mmval == nil {
			return ds.ErrNotFound
		}
		return f(mmval)
	})
}

func (bd *BoltDatastore) Has(key ds.Key) (bool, error) {
	var found bool
	err := bd.db.View(func(tx *bolt.Tx) error {
		val := tx.Bucket(bd.bucketName).Get(key.Bytes())
		found = val != nil
		return nil
	})
	return found, err
}

func (bd *BoltDatastore) GetSize(key ds.Key) (size int, err error) {
	size = -1
	err = bd.db.View(func(tx *bolt.Tx) error {
		val := tx.Bucket(bd.bucketName).Get(key.Bytes())
		if val == nil {
			return ds.ErrNotFound
		}
		size = len(val)
		return nil
	})
	return size, err
}

func (bd *BoltDatastore) Put(key ds.Key, val []byte) error {
	return bd.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bd.bucketName).Put(key.Bytes(), val)
	})
}

func (bd *BoltDatastore) Query(q query.Query) (query.Results, error) {
	// Special case order by key.
	orders := q.Orders
	if len(orders) > 0 {
		switch q.Orders[0].(type) {
		case query.OrderByKey, *query.OrderByKey:
			// Already ordered by key.
			orders = nil
		}
	}

	qrb := query.NewResultBuilder(q)
	qrb.Process.Go(func(worker goprocess.Process) {
		bd.db.View(func(tx *bolt.Tx) error {
			buck := tx.Bucket(bd.bucketName)
			c := buck.Cursor()

			var prefix []byte
			if qrb.Query.Prefix != "" {
				prefix = []byte(qrb.Query.Prefix)
			}

			// If we need to sort, we'll need to collect all the
			// results up-front.
			if len(orders) > 0 {
				// Query and filter.
				var entries []query.Entry
				for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
					dk := ds.NewKey(string(k)).String()
					e := query.Entry{Key: dk}
					if !qrb.Query.KeysOnly {
						// We copy _after_ filtering/sorting.
						e.Value = v
					}
					if filter(q.Filters, e) {
						continue
					}
					entries = append(entries, e)
				}

				// sort
				query.Sort(orders, entries)

				// offset/limit
				entries = entries[qrb.Query.Offset:]
				if qrb.Query.Limit > 0 {
					if qrb.Query.Limit < len(entries) {
						entries = entries[:qrb.Query.Limit]
					}
				}

				// Send
				for _, e := range entries {
					// Copy late so we don't have to copy
					// values we don't use.
					e.Value = append(e.Value[0:0:0], e.Value...)
					select {
					case qrb.Output <- query.Result{Entry: e}:
					case <-worker.Closing(): // client told us to end early.
						return nil
					}
				}
			} else {
				// Otherwise, send results as we get them.
				offset := 0
				for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
					dk := ds.NewKey(string(k)).String()
					e := query.Entry{Key: dk, Value: v}
					if !qrb.Query.KeysOnly {
						// We copy _after_ filtering.
						e.Value = v
					}

					// pre-filter
					if filter(q.Filters, e) {
						continue
					}

					// now count this item towards the results
					offset++

					// check the offset
					if offset < qrb.Query.Offset {
						continue
					}

					e.Value = append(e.Value[0:0:0], e.Value...)
					select {
					case qrb.Output <- query.Result{Entry: e}:
						offset++
					case <-worker.Closing():
						return nil
					}

					if qrb.Query.Limit > 0 &&
						offset >= (qrb.Query.Offset+qrb.Query.Limit) {
						// all done.
						return nil
					}
				}
			}
			return nil
		})
	})

	// go wait on the worker (without signaling close)
	go qrb.Process.CloseAfterChildren()

	return qrb.Results(), nil
}

// filter checks if we should filter out the query.
func filter(filters []query.Filter, entry query.Entry) bool {
	for _, filter := range filters {
		if !filter.Filter(entry) {
			return true
		}
	}
	return false
}

type boltBatch struct {
	tx  *bolt.Tx
	bkt *bolt.Bucket
}

func (bd *BoltDatastore) Batch() (ds.Batch, error) {
	tx, err := bd.db.Begin(true)
	if err != nil {
		return nil, err
	}

	buck := tx.Bucket(bd.bucketName)

	return &boltBatch{tx: tx, bkt: buck}, nil
}

func (bb *boltBatch) Commit() error {
	return bb.tx.Commit()
}

func (bb *boltBatch) Delete(k ds.Key) error {
	return bb.bkt.Delete(k.Bytes())
}

func (bb *boltBatch) Put(k ds.Key, val []byte) error {
	return bb.bkt.Put(k.Bytes(), val)
}

var _ ds.Batching = (*BoltDatastore)(nil)
