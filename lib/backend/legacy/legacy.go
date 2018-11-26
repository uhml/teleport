/*
Copyright 2016-2017 Gravitational, Inc.

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

// Package legacy stores previous, unsupported versions of the backend
// and is used for migrations
package legacy

import (
	"time"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
)

// Forever means that object TTL will not expire unless deleted
const (
	Forever         time.Duration = 0
	MaxLockDuration time.Duration = time.Minute
)

// Backend implements abstraction over local or remote storage backend
//
// Storage is modeled after BoltDB:
//  * bucket is a slice []string{"a", "b"}
//  * buckets contain key value pairs
//
type Backend interface {
	// GetKeys returns a list of keys for a given path
	GetKeys(bucket []string) ([]string, error)
	// GetItems returns a list of items (key value pairs) for a bucket.
	GetItems(bucket []string, opts ...OpOption) ([]Item, error)
	// CreateVal creates value with a given TTL and key in the bucket
	// if the value already exists, it must return trace.AlreadyExistsError
	CreateVal(bucket []string, key string, val []byte, ttl time.Duration) error
	// UpsertVal updates or inserts value with a given TTL into a bucket
	// ForeverTTL for no TTL
	UpsertVal(bucket []string, key string, val []byte, ttl time.Duration) error
	// UpsertItems updates or inserts all passed in backend.Items (with a TTL)
	// into the given bucket.
	UpsertItems(bucket []string, items []Item) error
	// GetVal return a value for a given key in the bucket
	GetVal(path []string, key string) ([]byte, error)
	// CompareAndSwapVal compares and swaps values in atomic operation,
	// succeeds if prevVal matches the value stored in the database,
	// requires prevVal as a non-empty value. Returns trace.CompareFailed
	// in case if value did not match.
	CompareAndSwapVal(bucket []string, key string, val []byte, prevVal []byte, ttl time.Duration) error
	// DeleteKey deletes a key in a bucket
	DeleteKey(bucket []string, key string) error
	// DeleteBucket deletes the bucket by a given path
	DeleteBucket(path []string, bkt string) error
	// AcquireLock grabs a lock that will be released automatically in TTL
	AcquireLock(token string, ttl time.Duration) error
	// ReleaseLock forces lock release before TTL
	ReleaseLock(token string) error
	// Close releases the resources taken up by this backend
	Close() error
	// Clock returns clock used by this backend
	Clock() clockwork.Clock
}

// OpConfig contains operation config
type OpConfig struct {
	// Recursive triggers recursive get
	Recursive bool
	// KeysOnly fetches only keys
	KeysOnly bool
}

// OpOption is operation functional argument
type OpOption func(*OpConfig) error

// WithRecursive sets get operation to be recursive
func WithRecursive() OpOption {
	return func(o *OpConfig) error {
		o.Recursive = true
		return nil
	}
}

// CollectOptions collects all options from functional
// arg and returns config
func CollectOptions(opts []OpOption) (*OpConfig, error) {
	cfg := OpConfig{}
	for _, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, trace.Wrap(err)
		}
	}
	return &cfg, nil
}

// Item is a pair of key and value.
type Item struct {
	// FullPath is set to full path
	FullPath string
	// Key is an item key.
	Key string
	// Value is an item value.
	Value []byte
	// TTL is the expire time for the item.
	TTL time.Duration
}

type Items []Item

// Len is part of sort.Interface.
func (it Items) Len() int {
	return len(it)
}

// Swap is part of sort.Interface.
func (it Items) Swap(i, j int) {
	it[i], it[j] = it[j], it[i]
}

// Less is part of sort.Interface.
func (it Items) Less(i, j int) bool {
	return it[i].Key < it[j].Key
}

// backend.Params type defines a flexible unified back-end configuration API.
// It is just a map of key/value pairs which gets populated by `storage` section
// in Teleport YAML config.
type Params map[string]interface{}

// NewFunc type defines a function type which every backend must implement to
// instantiate itself
type NewFunc func(Params) (Backend, error)

// NameFunc type defines a function type which every backend must implement
// to return its name
type NameFunc func() string

// Config is used for 'storage' config section. It's a combination of
// values for various backends: 'boltdb', 'etcd', 'filesystem' and 'dynamodb'
type Config struct {
	// Type can be "bolt" or "etcd" or "dynamodb"
	Type string `yaml:"type,omitempty"`

	// Params is a generic key/value property bag which allows arbitrary
	// falues to be passed to backend
	Params Params `yaml:",inline"`
}

// ValidateLockTTL helper allows all backends to validate lock TTL parameter
func ValidateLockTTL(ttl time.Duration) error {
	if ttl == Forever || ttl > MaxLockDuration {
		return trace.BadParameter("locks cannot exceed %v", MaxLockDuration)
	}
	return nil
}

// GetString returns a string value stored in Params map, or an empty string
// if nothing is found
func (p Params) GetString(key string) string {
	v, ok := p[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// TTL converts time to TTL from current time supplied
// by provider, if t is zero, returns forever
func TTL(clock clockwork.Clock, t time.Time) time.Duration {
	if t.IsZero() {
		return Forever
	}
	diff := t.UTC().Sub(clock.Now().UTC())
	if diff < 0 {
		return Forever
	}
	return diff
}

// AnyTTL returns TTL if any of the suplied times pass expiry time
// otherwise returns forever
func AnyTTL(clock clockwork.Clock, times ...time.Time) time.Duration {
	for _, t := range times {
		if !t.IsZero() {
			return TTL(clock, t)
		}
	}
	return Forever
}
