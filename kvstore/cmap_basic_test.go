package kvstore_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/h5law/kvwal"
	"github.com/h5law/kvwal/kvstore"
)

type kvPairs struct {
	key   kvwal.Key
	value kvwal.Value
}

func TestKVStore_Get(t *testing.T) {
	tests := []struct {
		desc          string
		entries       []*kvPairs
		key           kvwal.Key
		expectedValue kvwal.Value
		expectedErr   error
	}{
		{
			desc: "successful: get correct value",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:           []byte("key2"),
			expectedValue: []byte("value2"),
			expectedErr:   nil,
		},
		{
			desc: "failure: key not in store",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:           []byte("key5"),
			expectedValue: nil,
			expectedErr:   kvstore.ErrKeyNotFound,
		},
		{
			desc: "failure: key length zero",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:           nil,
			expectedValue: nil,
			expectedErr:   kvstore.ErrEmptyStoreKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			got, err := c.Get(tt.key)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(t, tt.expectedValue, got, "KVStore.Get(): got %s, want %s", got, tt.expectedValue)
		})
	}
}

func TestKVStore_GetAll(t *testing.T) {
	tests := []struct {
		desc           string
		entries        []*kvPairs
		expectedKeys   []kvwal.Key
		expectedValues []kvwal.Value
		expectedLen    int
	}{
		{
			desc: "successful: get all keys and values",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			expectedKeys:   []kvwal.Key{[]byte("key1"), []byte("key2"), []byte("key3"), []byte("key4")},
			expectedValues: []kvwal.Value{[]byte("value1"), []byte("value2"), []byte("value3"), []byte("value4")},
			expectedLen:    4,
		},
		{
			desc:           "successful: get all keys and values from empty store",
			entries:        []*kvPairs{},
			expectedKeys:   []kvwal.Key{},
			expectedValues: []kvwal.Value{},
			expectedLen:    0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			gotKeys, gotValues := c.GetAll()
			require.Equalf(t, tt.expectedLen, c.Len(), "KVStore.GetAll(): got %s, want %s", c.Len(), tt.expectedLen)
			require.Equalf(t, tt.expectedLen, len(gotKeys), "KVStore.GetAll(): got %s keys, want %s", len(gotKeys), tt.expectedLen)
			require.Equalf(t, tt.expectedLen, len(gotValues), "KVStore.GetAll(): got %s values, want %s", len(gotValues), tt.expectedLen)
			for i := range gotKeys {
				require.Truef(t, slices.ContainsFunc(tt.expectedKeys, func(k kvwal.Key) bool {
					return bytes.Equal(tt.expectedKeys[i], k)
				}), "KVStore.GetAll(): got unexpected key %s", gotKeys[i])
			}
			for i := range gotValues {
				require.Truef(t, slices.ContainsFunc(tt.expectedValues, func(v kvwal.Value) bool {
					return bytes.Equal(tt.expectedValues[i], v)
				}), "KVStore.GetAll(): got unexpected value %s", gotValues[i])
			}
		})
	}
}

func TestKVStore_GetPrefix(t *testing.T) {
	tests := []struct {
		desc           string
		entries        []*kvPairs
		prefix         kvwal.KeyPrefix
		expectedValues []kvwal.Value
	}{
		{
			desc: "successful: get prefixed values",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("prefix/key3"), value: []byte("value3")},
				{key: []byte("prefix/key4"), value: []byte("value4")},
			},
			prefix:         []byte("prefix/"),
			expectedValues: []kvwal.Value{[]byte("value3"), []byte("value4")},
		},
		{
			desc: "successful: get all values if prefix is nil",
			entries: []*kvPairs{
				{key: []byte("prefix/key1"), value: []byte("prefix/value1")},
				{key: []byte("prefix/key2"), value: []byte("prefix/value2")},
				{key: []byte("prefix/key3"), value: []byte("prefix/value3")},
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
			},
			prefix: nil,
			expectedValues: []kvwal.Value{
				[]byte("prefix/value1"),
				[]byte("prefix/value2"),
				[]byte("prefix/value3"),
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			got := c.GetPrefix(tt.prefix)
			require.Equalf(t, len(tt.expectedValues), len(got), "KVStore.GetPrefix(): got %d values, want %d", len(got), len(tt.expectedValues))
			for i := range got {
				require.Truef(t, slices.ContainsFunc(tt.expectedValues, func(v kvwal.Value) bool {
					return bytes.Equal(tt.expectedValues[i], v)
				}), "KVStore.GetPrefix(): got %s, want %s", got[i], tt.expectedValues)
			}
		})
	}
}

func TestKVStore_Has(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		key         kvwal.Key
		expectedHas bool
		expectedErr error
	}{
		{
			desc: "successful: has value",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:         []byte("key2"),
			expectedHas: true,
			expectedErr: nil,
		},
		{
			desc: "successful: does not have value",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:         []byte("key5"),
			expectedHas: false,
			expectedErr: nil,
		},
		{
			desc: "failure: key length zero",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:         nil,
			expectedHas: false,
			expectedErr: kvstore.ErrEmptyStoreKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			got, err := c.Has(tt.key)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(t, tt.expectedHas, got, "KVStore.Has(): got %s, want %s", got, tt.expectedHas)
		})
	}
}

func TestKVStore_Set(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		expectedLen int
		expectedErr error
	}{
		{
			desc: "successful: set distinct value",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
			},
			expectedLen: 1,
			expectedErr: nil,
		},
		{
			desc: "successful: set multiple distinct values",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			expectedLen: 4,
			expectedErr: nil,
		},
		{
			desc: "successful: set same value multiple times",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key1"), value: []byte("value2")},
			},
			expectedLen: 1,
			expectedErr: nil,
		},
		{
			desc: "failure: key length zero",
			entries: []*kvPairs{
				{key: nil, value: []byte("value1")},
			},
			expectedLen: 0,
			expectedErr: kvstore.ErrEmptyStoreKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				err := c.Set(entry.key, entry.value)
				if tt.expectedErr != nil {
					require.ErrorAs(t, err, &tt.expectedErr)
					return
				}
				require.NoError(t, err)
				got, err := c.Get(entry.key)
				require.NoError(t, err)
				require.Equalf(t, entry.value, got, "KVStore.Get(): got %s, want %s", got, entry.value)
			}
			require.Equalf(t, tt.expectedLen, c.Len(), "KVStore.Len(): got %d, want %d", c.Len(), tt.expectedLen)
		})
	}
}

func TestKVStore_Delete(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		key         kvwal.Key
		expectedLen int
		expectedErr error
	}{
		{
			desc: "successful: delete value",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:         []byte("key2"),
			expectedLen: 3,
			expectedErr: nil,
		},
		{
			desc: "failure: key not in store",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:         []byte("key5"),
			expectedLen: 4,
			expectedErr: kvstore.ErrKeyNotFound,
		},
		{
			desc: "failure: key length zero",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			key:         nil,
			expectedLen: 4,
			expectedErr: kvstore.ErrEmptyStoreKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			err := c.Delete(tt.key)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(t, tt.expectedLen, c.Len(), "KVStore.Len(): got %d, want %d", c.Len(), tt.expectedLen)
			has, err := c.Has(tt.key)
			require.NoError(t, err)
			require.Falsef(t, has, "KVStore.Has(): got %s, want %s", has, false)
		})
	}
}

func TestKVStore_DeletePrefix(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		prefix      kvwal.KeyPrefix
		expectedLen int
	}{
		{
			desc: "successful: delete prefixed values",
			entries: []*kvPairs{
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("prefix/key4"), []byte("value4")},
			},
			prefix:      []byte("prefix/"),
			expectedLen: 2,
		},
		{
			desc: "successful: delete all values if prefix is nil",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:      nil,
			expectedLen: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			c.DeletePrefix(tt.prefix)
			require.Equalf(t, tt.expectedLen, c.Len(), "KVStore.Len(): got %s, want %s", c.Len(), tt.expectedLen)
		})
	}
}

func TestKVStore_ClearAll(t *testing.T) {
	tests := []struct {
		desc    string
		entries []*kvPairs
	}{
		{
			desc: "successful: clear all values",
			entries: []*kvPairs{
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
				{[]byte("key4"), []byte("value4")},
			},
		},
		{
			desc:    "successful: clear empty store",
			entries: []*kvPairs{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			c.ClearAll()
			require.Equalf(t, 0, c.Len(), "KVStore.ClearAll(): got %s, want 0", c.Len())
		})
	}
}

func TestKVStore_Iterate(t *testing.T) {
	consumedList := make([]*kvPairs, 0)
	// consumerFn is a function that appends the key-value pair to the consumedList
	// if the key or value doesn't end with 3. Once it encounters a key or value
	// that ends with 3, it returns false to stop the iteration.
	consumerFn := func(key kvwal.Key, value kvwal.Value) bool {
		if bytes.HasSuffix(key, []byte("3")) || bytes.HasSuffix(value, []byte("3")) {
			return false
		}
		consumedList = append(consumedList, &kvPairs{key, value})
		return true
	}
	tests := []struct {
		desc                 string
		entries              []*kvPairs
		prefix               kvwal.KeyPrefix
		iterDirection        []kvwal.IterDirection
		expectedConsumedLen  int
		expectedConsumedList []*kvPairs
		expectedErr          error
	}{
		{
			desc: "successful: iterate forwards over prefixed keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("value1")},
				{[]byte("prefix/key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:              []byte("prefix/"),
			iterDirection:       []kvwal.IterDirection{kvwal.IterDirectionForward},
			expectedConsumedLen: 2,
			expectedConsumedList: []*kvPairs{
				{[]byte("prefix/key1"), []byte("value1")},
				{[]byte("prefix/key2"), []byte("value2")},
			},
			expectedErr: nil,
		},
		{
			desc: "successful: iterate forwards over all keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:              nil,
			iterDirection:       []kvwal.IterDirection{kvwal.IterDirectionForward},
			expectedConsumedLen: 2,
			expectedConsumedList: []*kvPairs{
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
			},
			expectedErr: nil,
		},
		{
			desc: "successful: iterate backwards over prefixed keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("value1")},
				{[]byte("prefix/key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:               []byte("prefix/"),
			iterDirection:        []kvwal.IterDirection{kvwal.IterDirectionReverse},
			expectedConsumedLen:  0, // 0 because the first key-value in reverse ends with 3
			expectedConsumedList: []*kvPairs{},
			expectedErr:          nil,
		},
		{
			desc: "successful: iterate backwards over all keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:               nil,
			iterDirection:        []kvwal.IterDirection{kvwal.IterDirectionReverse},
			expectedConsumedLen:  0, // 0 because the first key-value in reverse ends with 3
			expectedConsumedList: []*kvPairs{},
			expectedErr:          nil,
		},
		{
			desc: "failure: unknown iteration direction",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:               []byte("prefix/"),
			iterDirection:        []kvwal.IterDirection{kvwal.IterDirection(3)},
			expectedConsumedLen:  0,
			expectedConsumedList: []*kvPairs{},
			expectedErr:          kvstore.ErrUnknownIterDirection,
		},
		{
			desc: "failure: too many iteration directions",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix: []byte("prefix/"),
			iterDirection: []kvwal.IterDirection{
				kvwal.IterDirectionReverse,
				kvwal.IterDirectionReverse,
			},
			expectedConsumedLen:  0,
			expectedConsumedList: []*kvPairs{},
			expectedErr:          kvstore.ErrInvalidIterDirections,
		},
	}
	for _, tt := range tests {
		consumedList = make([]*kvPairs, 0)
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			err := c.Iterate(tt.prefix, consumerFn, tt.iterDirection...)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			for i := range consumedList {
				t.Logf("consumed key: %s, value: %s", consumedList[i].key, consumedList[i].value)
			}
			require.Equalf(
				t,
				tt.expectedConsumedLen,
				len(consumedList),
				"KVStore.Iterate() consumed items: got %d, want %d",
				tt.expectedConsumedLen,
				len(consumedList),
			)
			require.Equalf(
				t,
				tt.expectedConsumedList,
				consumedList,
				"KVStore.Iterate() consumed items: got %v, want %v",
				consumedList,
				tt.expectedConsumedList,
			)
		})
	}
}

func TestKVStore_IterateKeys(t *testing.T) {
	consumedList := make([]kvwal.Key, 0)
	// consumerFn is a function that appends the key to the consumedList if
	// the key doesn't end with 3. Once it encounters a key that ends with 3,
	// it returns false to stop the iteration.
	consumerFn := func(key kvwal.Key) bool {
		if bytes.HasSuffix(key, []byte("3")) {
			return false
		}
		consumedList = append(consumedList, key)
		return true
	}
	tests := []struct {
		desc                 string
		entries              []*kvPairs
		prefix               kvwal.KeyPrefix
		iterDirection        []kvwal.IterDirection
		expectedConsumedLen  int
		expectedConsumedList []kvwal.Key
		expectedErr          error
	}{
		{
			desc: "successful: iterate forwards over prefixed keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("value1")},
				{[]byte("prefix/key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:              []byte("prefix/"),
			iterDirection:       []kvwal.IterDirection{kvwal.IterDirectionForward},
			expectedConsumedLen: 2,
			expectedConsumedList: []kvwal.Key{
				kvwal.Key([]byte("prefix/key1")),
				kvwal.Key([]byte("prefix/key2")),
			},
			expectedErr: nil,
		},
		{
			desc: "successful: iterate forwards over all keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:              nil,
			iterDirection:       []kvwal.IterDirection{kvwal.IterDirectionForward},
			expectedConsumedLen: 2,
			expectedConsumedList: []kvwal.Key{
				kvwal.Key([]byte("key1")),
				kvwal.Key([]byte("key2")),
			},
			expectedErr: nil,
		},
		{
			desc: "successful: iterate backwards over prefixed keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("value1")},
				{[]byte("prefix/key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:               []byte("prefix/"),
			iterDirection:        []kvwal.IterDirection{kvwal.IterDirectionReverse},
			expectedConsumedLen:  0, // 0 because the first key-value in reverse ends with 3
			expectedConsumedList: []kvwal.Key{},
			expectedErr:          nil,
		},
		{
			desc: "successful: iterate backwards over all keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:               nil,
			iterDirection:        []kvwal.IterDirection{kvwal.IterDirectionReverse},
			expectedConsumedLen:  0, // 0 because the first key-value in reverse ends with 3
			expectedConsumedList: []kvwal.Key{},
			expectedErr:          nil,
		},
		{
			desc: "failure: unknown iteration direction",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:               []byte("prefix/"),
			iterDirection:        []kvwal.IterDirection{kvwal.IterDirection(3)},
			expectedConsumedLen:  0,
			expectedConsumedList: []kvwal.Key{},
			expectedErr:          kvstore.ErrUnknownIterDirection,
		},
		{
			desc: "failure: too many iteration directions",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix: []byte("prefix/"),
			iterDirection: []kvwal.IterDirection{
				kvwal.IterDirectionReverse,
				kvwal.IterDirectionReverse,
			},
			expectedConsumedLen:  0,
			expectedConsumedList: []kvwal.Key{},
			expectedErr:          kvstore.ErrInvalidIterDirections,
		},
	}
	for _, tt := range tests {
		consumedList = make([]kvwal.Key, 0)
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			err := c.IterateKeys(tt.prefix, consumerFn, tt.iterDirection...)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(
				t,
				tt.expectedConsumedLen,
				len(consumedList),
				"KVStore.IterateKeys() consumed items: got %d, want %d",
				tt.expectedConsumedLen,
				len(consumedList),
			)
			for i := range consumedList {
				require.Truef(t, slices.ContainsFunc(consumedList, func(k kvwal.Key) bool {
					return bytes.Equal(tt.expectedConsumedList[i], k)
				}), "KVStore.IterateKeys(): got unexpected key %s", consumedList[i])
			}
		})
	}
}

func TestKVStore_Len(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		expectedLen int
		expectedErr error
	}{
		{
			desc: "successful: has multiple values",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			expectedLen: 4,
			expectedErr: nil,
		},
		{
			desc:        "successful: empty store",
			entries:     []*kvPairs{},
			expectedLen: 0,
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			got := c.Len()
			require.Equalf(t, tt.expectedLen, got, "KVStore.Len(): got %d, want %d", got, tt.expectedLen)
		})
	}
}

func TestKVStore_Clone(t *testing.T) {
	tests := []struct {
		desc    string
		entries []*kvPairs
		equal   bool
	}{
		{
			desc:    "successful: clone empty store",
			entries: []*kvPairs{},
			equal:   true,
		},
		{
			desc: "successful: clone non-empty store",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("prefix/key2"), value: []byte("value2")},
				{key: []byte("prefix/key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			equal: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			d := c.Clone()
			eq, err := c.Equal(d)
			require.NoError(t, err)
			require.Equalf(t, tt.equal, eq, "KVStore.Clone(): got %v, want %v", eq, tt.equal)
		})
	}
}

func TestKVStore_Equal(t *testing.T) {
	tests := []struct {
		desc         string
		entries      []*kvPairs
		otherEntries []*kvPairs
		equal        bool
	}{
		{
			desc:         "successful: clone empty store",
			entries:      []*kvPairs{},
			otherEntries: []*kvPairs{},
			equal:        true,
		},
		{
			desc: "successful: clone non-empty store",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			otherEntries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			equal: true,
		},
		{
			desc: "failure: same keys, different values",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			otherEntries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value4")},
				{key: []byte("key2"), value: []byte("value3")},
				{key: []byte("key3"), value: []byte("value2")},
				{key: []byte("key4"), value: []byte("value1")},
			},
			equal: false,
		},
		{
			desc: "failure: different keys and values",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			otherEntries: []*kvPairs{
				{key: []byte("key5"), value: []byte("value5")},
				{key: []byte("key6"), value: []byte("value6")},
				{key: []byte("key7"), value: []byte("value7")},
				{key: []byte("key8"), value: []byte("value8")},
			},
			equal: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			d := kvstore.NewKVStore()
			for _, entry := range tt.otherEntries {
				require.NoError(t, d.Set(entry.key, entry.value))
			}
			eq, err := c.Equal(d)
			require.NoError(t, err)
			require.Equalf(t, tt.equal, eq, "KVStore.Equal(): got %v, want %v", eq, tt.equal)
		})
	}
}
