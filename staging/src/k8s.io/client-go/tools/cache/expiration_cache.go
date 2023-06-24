/*
Copyright 2014 The Kubernetes Authors.

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

package cache

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// ExpirationCache implements the store interface
//  1. All entries are automatically time stamped on insert
//     a. The key is computed based off the original item/keyFunc
//     b. The value inserted under that key is the timestamped item
//  2. Expiration happens lazily on read based on the expiration policy
//     a. No item can be inserted into the store while we're expiring
//     *any* item in the cache.
//  3. Time-stamps are stripped off unexpired entries before return
//
// Note that the ExpirationCache is inherently slower than a normal
// threadSafeStore because it takes a write lock every time it checks if
// an item has expired.
type ExpirationCacheT[V any] struct {
	cacheStorage     ThreadSafeStoreT[*TimestampedEntryT[V]]
	keyFunc          KeyFuncT[V]
	clock            clock.Clock
	expirationPolicy ExpirationPolicyT[V]
	// expirationLock is a write lock used to guarantee that we don't clobber
	// newly inserted objects because of a stale expiration timestamp comparison
	expirationLock sync.Mutex
}

type ExpirationCache = ExpirationCacheT[any]

// ExpirationPolicy dictates when an object expires. Currently only abstracted out
// so unittests don't rely on the system clock.
type ExpirationPolicyT[V any] interface {
	IsExpired(obj *TimestampedEntryT[V]) bool
}

type ExpirationPolicy = ExpirationPolicyT[any]

// TTLPolicy implements a ttl based ExpirationPolicy.
type TTLPolicyT[V any] struct {
	//	 >0: Expire entries with an age > ttl
	//	<=0: Don't expire any entry
	TTL time.Duration

	// Clock used to calculate ttl expiration
	Clock clock.Clock
}

type TTLPolicy = TTLPolicyT[any]

// IsExpired returns true if the given object is older than the ttl, or it can't
// determine its age.
func (p *TTLPolicyT[V]) IsExpired(obj *TimestampedEntryT[V]) bool {
	return p.TTL > 0 && p.Clock.Since(obj.Timestamp) > p.TTL
}

// TimestampedEntry is the only type allowed in a ExpirationCache.
// Keep in mind that it is not safe to share timestamps between computers.
// Behavior may be inconsistent if you get a timestamp from the API Server and
// use it on the client machine as part of your ExpirationCache.
type TimestampedEntryT[V any] struct {
	Obj       V
	Timestamp time.Time
	key       string
}

type TimestampedEntry = TimestampedEntryT[any]

// getTimestampedEntry returns the TimestampedEntry stored under the given key.
func (c *ExpirationCacheT[V]) getTimestampedEntry(key string) (*TimestampedEntryT[V], bool) {
	return c.cacheStorage.Get(key)
}

// getOrExpire retrieves the object from the TimestampedEntry if and only if it hasn't
// already expired. It holds a write lock across deletion.
func (c *ExpirationCacheT[V]) getOrExpire(key string) (V, bool) {
	// Prevent all inserts from the time we deem an item as "expired" to when we
	// delete it, so an un-expired item doesn't sneak in under the same key, just
	// before the Delete.
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()
	var zero V;
	timestampedItem, exists := c.getTimestampedEntry(key)
	if !exists {
		return zero, false
	}
	if c.expirationPolicy.IsExpired(timestampedItem) {
		c.cacheStorage.Delete(key)
		return zero, false
	}
	return timestampedItem.Obj, true
}

// GetByKey returns the item stored under the key, or sets exists=false.
func (c *ExpirationCacheT[V]) GetByKey(key string) (V, bool, error) {
	obj, exists := c.getOrExpire(key)
	return obj, exists, nil
}

// Get returns unexpired items. It purges the cache of expired items in the
// process.
func (c *ExpirationCacheT[V]) Get(obj V) (V, bool, error) {
	key, err := c.keyFunc(obj)
	var zero V;
	if err != nil {
		return zero, false, KeyError{obj, err}
	}
	obj, exists := c.getOrExpire(key)
	return obj, exists, nil
}

// List retrieves a list of unexpired items. It purges the cache of expired
// items in the process.
func (c *ExpirationCacheT[V]) List() []V {
	items := c.cacheStorage.List()

	list := make([]V, 0, len(items))
	for _, item := range items {
		key := item.key
		if obj, exists := c.getOrExpire(key); exists {
			list = append(list, obj)
		}
	}
	return list
}

// ListKeys returns a list of all keys in the expiration cache.
func (c *ExpirationCacheT[V]) ListKeys() []string {
	return c.cacheStorage.ListKeys()
}

// Add timestamps an item and inserts it into the cache, overwriting entries
// that might exist under the same key.
func (c *ExpirationCacheT[V]) Add(obj V) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()

	c.cacheStorage.Add(key, &TimestampedEntryT[V]{obj, c.clock.Now(), key})
	return nil
}

// Update has not been implemented yet for lack of a use case, so this method
// simply calls `Add`. This effectively refreshes the timestamp.
func (c *ExpirationCacheT[V]) Update(obj V) error {
	return c.Add(obj)
}

// Delete removes an item from the cache.
func (c *ExpirationCacheT[V]) Delete(obj V) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()
	c.cacheStorage.Delete(key)
	return nil
}

// Replace will convert all items in the given list to TimestampedEntries
// before attempting the replace operation. The replace operation will
// delete the contents of the ExpirationCache `c`.
func (c *ExpirationCacheT[V]) Replace(list []V, resourceVersion string) error {
	items := make(map[string]*TimestampedEntryT[V], len(list))
	ts := c.clock.Now()
	for _, item := range list {
		key, err := c.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		items[key] = &TimestampedEntryT[V]{item, ts, key}
	}
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()
	c.cacheStorage.Replace(items, resourceVersion)
	return nil
}

// Resync is a no-op for one of these
func (c *ExpirationCacheT[V]) Resync() error {
	return nil
}

// NewTTLStore creates and returns a ExpirationCache with a TTLPolicy
func NewTTLStore(keyFunc KeyFunc, ttl time.Duration) Store {
	return NewExpirationStore(keyFunc, &TTLPolicy{ttl, clock.RealClock{}})
}

// NewExpirationStore creates and returns a ExpirationCache for a given policy
func NewExpirationStore(keyFunc KeyFunc, expirationPolicy ExpirationPolicy) Store {
	return &ExpirationCache{
		cacheStorage:     NewThreadSafeStoreT[*TimestampedEntry](IndexersT[*TimestampedEntry]{}, Indices{}),
		keyFunc:          keyFunc,
		clock:            clock.RealClock{},
		expirationPolicy: expirationPolicy,
	}
}
