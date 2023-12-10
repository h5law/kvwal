package kvstore_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/h5law/kvwal/kvstore"
)

// TODO: Increase coverage of the KVStore interface methods using concurrent
// behaviour. Currently, only the Get, Set, and Delete methods are tested.

func TestKVStore_Concurrent_Get(t *testing.T) {
	// Create a new instance of the KVStore interface.
	kv := kvstore.NewKVStore()

	// Set some values.
	require.NoError(t, kv.Set([]byte("key1"), []byte("value1")))
	require.NoError(t, kv.Set([]byte("key2"), []byte("value2")))
	require.NoError(t, kv.Set([]byte("key3"), []byte("value3")))
	require.NoError(t, kv.Set([]byte("key4"), []byte("value4")))

	// Create a new wait group.
	wg := sync.WaitGroup{}

	// Start the goroutines.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			key := fmt.Sprintf("key%d", i+1)
			value, err := kv.Get([]byte(key))
			require.NoError(t, err)
			require.Truef(
				t,
				bytes.Equal([]byte(fmt.Sprintf("value%d", i+1)), value),
				"KVStore.Get() Concurrent: expected %s, got %s",
				fmt.Sprintf("value%d", i+1),
				value,
			)
		}(i)
	}

	// Wait for all goroutines to finish.
	wg.Wait()
}

func TestKVStore_Concurrent_Set(t *testing.T) {
	// Create a new instance of the KVStore interface.
	kv := kvstore.NewKVStore()

	entries := []*kvPairs{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
	}

	// Create a new wait group.
	wg := sync.WaitGroup{}

	// Start the goroutines.
	for _, entry := range entries {
		wg.Add(1)
		go func(entry *kvPairs) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			require.NoError(t, kv.Set(entry.key, entry.value))
		}(entry)
	}

	// Wait for all goroutines to finish.
	wg.Wait()

	// Check that all values were set correctly.
	for _, entry := range entries {
		value, err := kv.Get(entry.key)
		require.NoError(t, err)
		require.Truef(
			t,
			bytes.Equal(entry.value, value),
			"KVStore.Set() Concurrent: expected %s, got %s",
			entry.value,
			value,
		)
	}
}

func TestKVStore_Concurrent_Overwrite(t *testing.T) {
	// Create a new instance of the KVStore interface.
	kv := kvstore.NewKVStore()

	entries := []*kvPairs{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
		{key: []byte("key4"), value: []byte("value4")},
		{key: []byte("key1"), value: []byte("value5")},
		{key: []byte("key2"), value: []byte("value6")},
	}

	// Create a new wait group.
	wg := sync.WaitGroup{}

	// Sleep times
	sleepTimes := make([]int, len(entries))

	// Start the goroutines.
	for i := 0; i < len(entries); i++ {
		j := i
		st := rand.Intn(5)
		sleepTimes[j] = st
		wg.Add(1)
		go func(j, st int) {
			defer wg.Done()
			time.Sleep(time.Duration(st) * time.Millisecond)
			require.NoError(t, kv.Set(entries[j].key, entries[j].value))
		}(j, st)
	}

	// Wait for all goroutines to finish.
	wg.Wait()

	// Check that all values were set correctly.
	allKeys, allValues := kv.GetAll()
	require.Equalf(t, 4, len(allKeys), "KVStore.Set() Concurrent: got %d keys, want %d", len(allKeys), 4)
	require.Equalf(t, 4, len(allValues), "KVStore.Set() Concurrent: got %d values, want %d", len(allValues), 4)

	// Check the overwritten values based on the sleep times.
	val1, err := kv.Get([]byte("key1"))
	require.NoError(t, err)
	if sleepTimes[0] > sleepTimes[4] {
		require.Truef(t, bytes.Equal([]byte("value1"), val1), "KVStore.Set() Concurrent: got %s, want %s", val1, []byte("value1"))
	} else if sleepTimes[0] < sleepTimes[4] {
		require.Truef(t, bytes.Equal([]byte("value5"), val1), "KVStore.Set() Concurrent: got %s, want %s", val1, []byte("value5"))
	}
	// If the sleep times are equal, then the last value cannot be determined
	// deterministically.

	val2, err := kv.Get([]byte("key2"))
	require.NoError(t, err)
	if sleepTimes[1] > sleepTimes[5] {
		require.Truef(t, bytes.Equal([]byte("value2"), val2), "KVStore.Set() Concurrent: got %s, want %s", val2, []byte("value2"))
	} else if sleepTimes[1] < sleepTimes[5] {
		require.Truef(t, bytes.Equal([]byte("value6"), val2), "KVStore.Set() Concurrent: got %s, want %s", val2, []byte("value6"))
	}
	// If the sleep times are equal, then the last value cannot be determined
	// deterministically.
}

func TestKVStore_Concurrent_Delete(t *testing.T) {
	// Create a new instance of the KVStore interface.
	kv := kvstore.NewKVStore()

	// Fill the KVStore with some values.
	require.NoError(t, kv.Set([]byte("key1"), []byte("value1")))
	require.NoError(t, kv.Set([]byte("key2"), []byte("value2")))
	require.NoError(t, kv.Set([]byte("key3"), []byte("value3")))
	require.NoError(t, kv.Set([]byte("key4"), []byte("value4")))

	// Create a new wait group.
	wg := sync.WaitGroup{}

	// Start the goroutines.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			key := fmt.Sprintf("key%d", i+1)
			require.NoError(t, kv.Delete([]byte(key)))
		}(i)
	}

	// Wait for all goroutines to finish.
	wg.Wait()

	// Check that all values were deleted correctly.
	require.Equalf(t, 0, kv.Len(), "KVStore.Delete() Concurrent: got %d keys remaining, want %d", kv.Len(), 0)
}

func TestKVStore_Concurrent_DeleteTwice(t *testing.T) {
	// Create a new instance of the KVStore interface.
	kv := kvstore.NewKVStore()

	// Fill the KVStore with some values.
	require.NoError(t, kv.Set([]byte("key1"), []byte("value1")))
	require.NoError(t, kv.Set([]byte("key2"), []byte("value2")))
	require.NoError(t, kv.Set([]byte("key3"), []byte("value3")))
	require.NoError(t, kv.Set([]byte("key4"), []byte("value4")))

	// Create a new err group.
	eg := errgroup.Group{}
	for i := 0; i < 5; i++ {
		j := i
		// Start the goroutines.
		eg.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			key := fmt.Sprintf("key%d", (j+1)%4) // this will circle back and delete key1 again
			return kv.Delete([]byte(key))
		})
	}

	// Wait for all goroutines to finish.
	err := eg.Wait()
	require.ErrorAs(t, err, &kvstore.ErrKeyNotFound, "KVStore.Delete() Concurrent: expected ErrKeyNotFound, got %v", err)
}
