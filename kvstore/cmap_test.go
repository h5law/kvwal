package kvstore_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/h5law/kvwal/kvstore"
)

type kvPairs struct {
	key   kvstore.Key
	value kvstore.Value
}

func TestKVStore_Equal(t *testing.T) {
	tests := []struct {
		desc         string
		entries      []*kvPairs
		otherEntries []*kvPairs
		equal        bool
	}{
		{
			desc:    "successful: clone empty store",
			entries: []*kvPairs{},
			equal:   true,
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

func TestKVStore_Get(t *testing.T) {
	type args struct {
		key kvstore.Key
	}
	tests := []struct {
		desc          string
		entries       []*kvPairs
		key           kvstore.Key
		expectedValue kvstore.Value
		expectedErr   error
	}{
		{
			desc: "successful: get value",
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

func TestKVStore_GetPrefix(t *testing.T) {
	tests := []struct {
		desc           string
		entries        []*kvPairs
		prefix         kvstore.KeyPrefix
		expectedValues []kvstore.Value
		expectedErr    error
	}{
		{
			desc: "successful: get prefixed values",
			entries: []*kvPairs{
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("prefix/key4"), []byte("value4")},
			},
			prefix:         []byte("prefix/"),
			expectedValues: []kvstore.Value{[]byte("value3"), []byte("value4")},
			expectedErr:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			got, err := c.GetPrefix(tt.prefix)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(t, tt.expectedValues, got, "KVStore.GetPrefix(): got %s, want %s", got, tt.expectedValues)
		})
	}
}

func TestKVStore_Has(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		key         kvstore.Key
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
			}
			require.Equalf(t, tt.expectedLen, c.Len(), "KVStore.Len(): got %d, want %d", c.Len(), tt.expectedLen)
		})
	}
}

func TestKVStore_Delete(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		key         kvstore.Key
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
		})
	}
}

func TestKVStore_DeletePrefix(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		prefix      kvstore.KeyPrefix
		expectedLen int
		expectedErr error
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
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			err := c.DeletePrefix(tt.prefix)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(t, tt.expectedLen, c.Len(), "KVStore.Len(): got %s, want %s", c.Len(), tt.expectedLen)
		})
	}
}

func TestKVStore_ClearAll(t *testing.T) {
	tests := []struct {
		desc        string
		entries     []*kvPairs
		expectedErr error
	}{
		{
			desc: "successful: clear all values",
			entries: []*kvPairs{
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
				{[]byte("key4"), []byte("value4")},
			},
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			err := c.ClearAll()
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(t, 0, c.Len(), "KVStore.ClearAll(): got %s, want 0", c.Len())
		})
	}
}

func TestKVStore_Iterate(t *testing.T) {
	consumedList := make([]*kvPairs, 0)
	consumerFn := func(key kvstore.Key, value kvstore.Value) bool {
		if bytes.HasSuffix(key, []byte("3")) || bytes.HasSuffix(value, []byte("3")) {
			return false
		}
		consumedList = append(consumedList, &kvPairs{key, value})
		return true
	}
	tests := []struct {
		desc                string
		entries             []*kvPairs
		prefix              kvstore.KeyPrefix
		expectedConsumedLen int
		expectedErr         error
	}{
		{
			desc: "successful: iterate over prefixed keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("value1")},
				{[]byte("prefix/key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:              []byte("prefix/"),
			expectedConsumedLen: 2,
			expectedErr:         nil,
		},
		{
			desc: "successful: iterate over all keys and values",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:              nil,
			expectedConsumedLen: 2,
			expectedErr:         nil,
		},
	}
	for _, tt := range tests {
		consumedList = make([]*kvPairs, 0)
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			err := c.Iterate(tt.prefix, consumerFn)
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(
				t,
				tt.expectedConsumedLen,
				len(consumedList),
				"KVStore.Iterate() consumed items: got %d, want %d",
				tt.expectedConsumedLen,
				len(consumedList),
			)
		})
	}
}

func TestKVStore_IterateKeys(t *testing.T) {
	consumedList := make([]kvstore.Key, 0)
	consumerFn := func(key kvstore.Key) bool {
		if bytes.HasSuffix(key, []byte("3")) {
			return false
		}
		consumedList = append(consumedList, key)
		return true
	}
	tests := []struct {
		desc                string
		entries             []*kvPairs
		prefix              kvstore.KeyPrefix
		expectedConsumedLen int
		expectedErr         error
	}{
		{
			desc: "successful: iterate over prefixed keys",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("value1")},
				{[]byte("prefix/key2"), []byte("value2")},
				{[]byte("prefix/key3"), []byte("value3")},
				{[]byte("key4"), []byte("value4")},
				{[]byte("key5"), []byte("value5")},
				{[]byte("key6"), []byte("value6")},
			},
			prefix:              []byte("prefix/"),
			expectedConsumedLen: 5,
			expectedErr:         nil,
		},
		{
			desc: "successful: iterate over all keys",
			entries: []*kvPairs{
				{[]byte("prefix/key1"), []byte("prefix/value1")},
				{[]byte("prefix/key2"), []byte("prefix/value2")},
				{[]byte("prefix/key3"), []byte("prefix/value3")},
				{[]byte("key1"), []byte("value1")},
				{[]byte("key2"), []byte("value2")},
				{[]byte("key3"), []byte("value3")},
			},
			prefix:              nil,
			expectedConsumedLen: 2,
			expectedErr:         nil,
		},
	}
	for _, tt := range tests {
		consumedList = make([]kvstore.Key, 0)
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				require.NoError(t, c.Set(entry.key, entry.value))
			}
			err := c.IterateKeys(nil, consumerFn)
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
			desc: "successful: has value",
			entries: []*kvPairs{
				{key: []byte("key1"), value: []byte("value1")},
				{key: []byte("key2"), value: []byte("value2")},
				{key: []byte("key3"), value: []byte("value3")},
				{key: []byte("key4"), value: []byte("value4")},
			},
			expectedLen: 4,
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
		desc        string
		entries     []*kvPairs
		equal       bool
		expectedErr error
	}{
		{
			desc:    "successful: clone empty store",
			entries: []*kvPairs{},
			equal:   true,
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
			if tt.expectedErr != nil {
				require.ErrorAs(t, err, &tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equalf(t, tt.equal, eq, "KVStore.Clone(): got %v, want %v", eq, tt.equal)
		})
	}
}
