/*
Copyright 2015 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package boltbk

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"

	"github.com/gravitational/teleport"
	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/backend/legacy"
	"github.com/gravitational/teleport/lib/utils"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
)

const (
	// keysBoltFile is the BoltDB database file, usually stored in data_dir
	keysBoltFile = "keys.db"

	// openTimeout determines for how long BoltDB will wait before giving up
	// opening the locked DB file
	openTimeout = 5 * time.Second

	// openFileMode flag is passed to db.Open()
	openFileMode = 0600
)

// BoltBackend is a boltdb-based backend used in tests and standalone mode
type BoltBackend struct {
	sync.Mutex

	db    *bolt.DB
	clock clockwork.Clock
	locks map[string]time.Time
}

// GetName() is a part of the backend API and returns the name of this backend
// as shown in 'storage/type' section of Teleport YAML config
func GetName() string {
	return "bolt"
}

// Exists returns true if backend has been used before
func Exists(path string) (bool, error) {
	path, err := filepath.Abs(filepath.Join(path, keysBoltFile))
	if err != nil {
		return false, trace.Wrap(err)
	}
	f, err := os.Open(path)
	err = trace.ConvertSystemError(err)
	if err != nil {
		if trace.IsNotFound(err) {
			return false, nil
		}
		return false, trace.Wrap(err)
	}
	defer f.Close()
	return true, nil
}

// New initializes and returns a fully created BoltDB backend. It's
// a properly implemented Backend.NewFunc, part of a backend API
func New(params legacy.Params) (*BoltBackend, error) {
	// look at 'path' parameter, if it's missing use 'data_dir' (default):
	path := params.GetString("path")
	if len(path) == 0 {
		path = params.GetString(teleport.DataDirParameterName)
	}
	// still nothing? return an error:
	if path == "" {
		return nil, trace.BadParameter("BoltDB backend: 'path' is not set")
	}
	if !utils.IsDir(path) {
		return nil, trace.BadParameter("%v is not a valid directory", path)
	}
	path, err := filepath.Abs(filepath.Join(path, keysBoltFile))
	if err != nil {
		return nil, trace.Wrap(err)
	}

	db, err := bolt.Open(path, openFileMode, &bolt.Options{Timeout: openTimeout})
	if err != nil {
		if err == bolt.ErrTimeout {
			return nil, trace.Errorf("Local storage is locked. Another instance is running? (%v)", path)
		}
		return nil, trace.Wrap(err)
	}

	// Wrap the backend in a input sanitizer and return it.
	return &BoltBackend{
		locks: make(map[string]time.Time),
		clock: clockwork.NewRealClock(),
		db:    db,
	}, nil
}

// Clock returns clock assigned to the backend
func (b *BoltBackend) Clock() clockwork.Clock {
	return b.clock
}

// Close closes the backend resources
func (b *BoltBackend) Close() error {
	return b.db.Close()
}

type path struct {
	bucket  []string
	key     string
	realKey string
}

type fetcher struct {
	bucket []string
	paths  []path
}

func (f *fetcher) fetchBucket(tx *bolt.Tx, bucket []string) error {
	bkt, err := GetBucket(tx, bucket)
	if err != nil {
		berr := boltErr(err)
		if !trace.IsNotFound(berr) {
			return trace.Wrap(berr)
		}
		return nil
	}
	c := bkt.Cursor()
	for k, val := c.First(); k != nil; k, val = c.Next() {
		if val != nil {
			// If bucket path on disk and the requested bucket were an exact match,
			// return the key as-is.
			//
			// However, if this was a partial match, for example pathToBucket is
			// "/roles/admin/params" but the bucketPrefix is "/roles" then extract
			// the first suffix (in this case "admin") and use this as the key. This
			// is consistent with our DynamoDB implementation.
			var p path
			if len(f.bucket) == len(bucket) {
				p.realKey = string(k)
			} else {
				p.realKey = bucket[len(f.bucket)]
			}
			p.bucket = bucket
			p.key = string(k)
			f.paths = append(f.paths, p)
		} else {
			// this is a bucket
			b := append([]string{}, bucket...)
			b = append(b, string(k))
			err := f.fetchBucket(tx, b)
			if err != nil {
				return trace.Wrap(err)
			}
		}
	}
	return nil
}

func (f *fetcher) fetch(tx *bolt.Tx) error {
	return f.fetchBucket(tx, f.bucket)
}

func (b *BoltBackend) getItemsRecursive(bucket []string) ([]legacy.Item, error) {
	f := &fetcher{
		bucket: bucket,
	}

	err := b.db.View(f.fetch)
	if err != nil {
		return nil, trace.Wrap(boltErr(err))
	}

	// This is a very inefficient approach. It's here to satisfy the
	// backend.Backend interface since the Bolt backend is slated for removal
	// in 2.7.0 anyway.
	items := make([]legacy.Item, 0, len(f.paths))
	for _, p := range f.paths {
		val, err := b.GetVal(p.bucket, p.key)
		if err != nil {
			continue
		}
		items = append(items, legacy.Item{
			Key:   p.realKey,
			Value: val,
		})

	}
	return items, nil
}

// Export exports all items from the backend in new backend Items
func (b *BoltBackend) Export() ([]backend.Item, error) {
	var rootBuckets [][]string
	err := b.db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			rootBuckets = append(rootBuckets, []string{string(name)})
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}

	var items []backend.Item
	for _, bucket := range rootBuckets {
		f := &fetcher{
			bucket: bucket,
		}

		err := b.db.View(f.fetch)
		if err != nil {
			return nil, trace.Wrap(boltErr(err))
		}

		for _, p := range f.paths {
			item, err := b.getItem(p.bucket, p.key)
			if err != nil {
				continue
			}
			items = append(items, *item)
		}
	}
	return items, nil
}

func (b *BoltBackend) getItem(path []string, key string) (*backend.Item, error) {
	var val []byte
	if err := b.getKey(path, key, &val); err != nil {
		return nil, trace.Wrap(boltErr(err))
	}
	var k *kv
	if err := json.Unmarshal(val, &k); err != nil {
		return nil, trace.Wrap(err)
	}
	if k.TTL != 0 && b.clock.Now().UTC().Sub(k.Created) > k.TTL {
		if err := b.deleteKey(path, key); err != nil {
			return nil, err
		}
		return nil, trace.NotFound("%v: %v not found", path, key)
	}
	flatKey := make([]string, len(path))
	copy(flatKey, path)
	flatKey = append(flatKey, key)
	item := backend.Item{
		Key:   backend.Key(flatKey...),
		Value: k.Value,
	}
	if k.TTL != 0 {
		item.Expires = b.clock.Now().UTC().Add(k.TTL)
	}
	return &item, nil
}

// GetItems fetches keys and values and returns them to the caller.
func (b *BoltBackend) GetItems(path []string, opts ...legacy.OpOption) ([]legacy.Item, error) {
	cfg, err := legacy.CollectOptions(opts)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	if cfg.Recursive {
		return b.getItemsRecursive(path)
	}

	keys, err := b.GetKeys(path)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	// This is a very inefficient approach. It's here to satisfy the
	// backend.Backend interface since the Bolt backend is slated for removal
	// in 2.7.0 anyway.
	items := make([]legacy.Item, 0, len(keys))
	for _, e := range keys {
		val, err := b.GetVal(path, e)
		if err != nil {
			continue
		}

		items = append(items, legacy.Item{
			Key:   e,
			Value: val,
		})
	}

	return items, nil
}

func (b *BoltBackend) GetKeys(path []string) ([]string, error) {
	keys, err := b.getKeys(path)
	if err != nil {
		if trace.IsNotFound(err) {
			return nil, nil
		}
		return nil, trace.Wrap(err)
	}
	// now do an iteration to expire keys
	for _, key := range keys {
		b.GetVal(path, key)
	}
	keys, err = b.getKeys(path)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	sort.Sort(sort.StringSlice(keys))
	return keys, nil
}

func (bk *BoltBackend) UpsertItems(bucket []string, items []legacy.Item) error {
	return trace.BadParameter("not implemented")
}

func (b *BoltBackend) UpsertVal(path []string, key string, val []byte, ttl time.Duration) error {
	return b.upsertVal(path, key, val, ttl)
}

func (b *BoltBackend) CreateVal(bucket []string, key string, val []byte, ttl time.Duration) error {
	v := &kv{
		Created: b.clock.Now().UTC(),
		Value:   val,
		TTL:     ttl,
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		return trace.Wrap(err)
	}
	err = b.createKey(bucket, key, bytes)
	return trace.Wrap(err)
}

// CompareAndSwapVal compares and swap values in atomic operation,
// succeeds if prevData matches the value stored in the databases,
// requires prevData as a non-empty value. Returns trace.CompareFailed
// in case if value did not match
func (b *BoltBackend) CompareAndSwapVal(bucket []string, key string, newData []byte, prevData []byte, ttl time.Duration) error {
	if len(prevData) == 0 {
		return trace.BadParameter("missing prevData parameter, to atomically create item, use CreateVal method")
	}
	v := &kv{
		Created: b.clock.Now().UTC(),
		Value:   newData,
		TTL:     ttl,
	}
	newEncodedData, err := json.Marshal(v)
	if err != nil {
		return trace.Wrap(err)
	}
	err = b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := GetBucket(tx, bucket)
		if err != nil {
			if trace.IsNotFound(err) {
				return trace.CompareFailed("key %q is not found", key)
			}
			return trace.Wrap(err)
		}
		currentData := bkt.Get([]byte(key))
		if currentData == nil {
			_, err := GetBucket(tx, append(bucket, key))
			if err == nil {
				return trace.BadParameter("key %q is a bucket", key)
			}
			return trace.CompareFailed("%v %v is not found", bucket, key)
		}
		var currentVal kv
		if err := json.Unmarshal(currentData, &currentVal); err != nil {
			return trace.Wrap(err)
		}
		if bytes.Compare(prevData, currentVal.Value) != 0 {
			return trace.CompareFailed("%q is not matching expected value", key)
		}
		return boltErr(bkt.Put([]byte(key), newEncodedData))
	})
	return trace.Wrap(err)
}

func (b *BoltBackend) upsertVal(path []string, key string, val []byte, ttl time.Duration) error {
	v := &kv{
		Created: b.clock.Now().UTC(),
		Value:   val,
		TTL:     ttl,
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		return trace.Wrap(err)
	}
	return b.upsertKey(path, key, bytes)
}

func (b *BoltBackend) GetVal(path []string, key string) ([]byte, error) {
	var val []byte
	if err := b.getKey(path, key, &val); err != nil {
		return nil, trace.Wrap(boltErr(err))
	}
	var k *kv
	if err := json.Unmarshal(val, &k); err != nil {
		return nil, trace.Wrap(err)
	}
	if k.TTL != 0 && b.clock.Now().UTC().Sub(k.Created) > k.TTL {
		if err := b.deleteKey(path, key); err != nil {
			return nil, err
		}
		return nil, trace.NotFound("%v: %v not found", path, key)
	}
	return k.Value, nil
}

func (b *BoltBackend) DeleteKey(path []string, key string) error {
	b.Lock()
	defer b.Unlock()
	return b.deleteKey(path, key)
}

func (b *BoltBackend) DeleteBucket(path []string, bucket string) error {
	b.Lock()
	defer b.Unlock()
	return b.deleteBucket(path, bucket)
}

func (b *BoltBackend) deleteBucket(buckets []string, bucket string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		if len(buckets) == 0 {
			return boltErr(tx.DeleteBucket([]byte(bucket)))
		}
		bkt, err := GetBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		if bkt.Bucket([]byte(bucket)) == nil {
			return trace.NotFound("%v not found", bucket)
		}
		return boltErr(bkt.DeleteBucket([]byte(bucket)))
	})
}

func (b *BoltBackend) deleteKey(buckets []string, key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := GetBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		if bkt.Get([]byte(key)) == nil {
			return trace.NotFound("%v is not found", key)
		}
		return boltErr(bkt.Delete([]byte(key)))
	})
}

func (b *BoltBackend) upsertKey(buckets []string, key string, bytes []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := UpsertBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		return boltErr(bkt.Put([]byte(key), bytes))
	})
}

func (b *BoltBackend) createKey(buckets []string, key string, bytes []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := UpsertBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		val := bkt.Get([]byte(key))
		if val != nil {
			return trace.AlreadyExists("'%v' already exists", key)
		}
		return boltErr(bkt.Put([]byte(key), bytes))
	})
}

func (b *BoltBackend) upsertJSONKey(buckets []string, key string, val interface{}) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return trace.Wrap(err)
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := UpsertBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		return boltErr(bkt.Put([]byte(key), bytes))
	})
}

func (b *BoltBackend) getJSONKey(buckets []string, key string, val interface{}) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bkt, err := GetBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		bytes := bkt.Get([]byte(key))
		if bytes == nil {
			return trace.NotFound("%v %v not found", buckets, key)
		}
		return json.Unmarshal(bytes, val)
	})
}

func (b *BoltBackend) getKey(buckets []string, key string, val *[]byte) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bkt, err := GetBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		bytes := bkt.Get([]byte(key))
		if bytes == nil {
			_, err := GetBucket(tx, append(buckets, key))
			if err == nil {
				return trace.BadParameter("key %q is a bucket", key)
			}
			return trace.NotFound("%v %v not found", buckets, key)
		}
		*val = make([]byte, len(bytes))
		copy(*val, bytes)
		return nil
	})
}

func (b *BoltBackend) getKeys(buckets []string) ([]string, error) {
	out := []string{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bkt, err := GetBucket(tx, buckets)
		if err != nil {
			return trace.Wrap(err)
		}
		c := bkt.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			out = append(out, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, trace.Wrap(boltErr(err))
	}
	return out, nil
}

func UpsertBucket(b *bolt.Tx, buckets []string) (*bolt.Bucket, error) {
	bkt, err := b.CreateBucketIfNotExists([]byte(buckets[0]))
	if err != nil {
		return nil, trace.Wrap(err)
	}
	for _, key := range buckets[1:] {
		bkt, err = bkt.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	return bkt, nil
}

func GetBucket(b *bolt.Tx, buckets []string) (*bolt.Bucket, error) {
	bkt := b.Bucket([]byte(buckets[0]))
	if bkt == nil {
		return nil, trace.NotFound("bucket %v not found", buckets[0])
	}
	for _, key := range buckets[1:] {
		bkt = bkt.Bucket([]byte(key))
		if bkt == nil {
			return nil, trace.NotFound("bucket %v not found", key)
		}
	}
	return bkt, nil
}

func (b *BoltBackend) AcquireLock(token string, ttl time.Duration) error {
	for {
		b.Lock()
		expires, ok := b.locks[token]
		if ok && (expires.IsZero() || expires.After(b.clock.Now().UTC())) {
			b.Unlock()
			b.clock.Sleep(100 * time.Millisecond)
		} else {
			if ttl == 0 {
				b.locks[token] = time.Time{}
			} else {
				b.locks[token] = b.clock.Now().UTC().Add(ttl)
			}
			b.Unlock()
			return nil
		}
	}
}

func (b *BoltBackend) ReleaseLock(token string) error {
	b.Lock()
	defer b.Unlock()

	expires, ok := b.locks[token]
	if !ok || (!expires.IsZero() && expires.Before(b.clock.Now().UTC())) {
		return trace.NotFound("lock %v is deleted or expired", token)
	}
	delete(b.locks, token)
	return nil
}

type kv struct {
	Created time.Time     `json:"created"`
	TTL     time.Duration `json:"ttl"`
	Value   []byte        `json:"val"`
}

func boltErr(err error) error {
	if err == bolt.ErrBucketNotFound {
		return trace.NotFound(err.Error())
	}
	if err == bolt.ErrBucketExists {
		return trace.AlreadyExists(err.Error())
	}
	if err == bolt.ErrDatabaseNotOpen {
		return trace.ConnectionProblem(err, "database is not open")
	}
	return err
}
