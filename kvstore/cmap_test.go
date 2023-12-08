package kvstore_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/h5law/kvwal/kvstore"
)

type kvPairs struct {
	key   kvstore.Key
	value kvstore.Value
}

type key struct {
	key kvstore.Key
}

type prefix struct {
	key kvstore.KeyPrefix
}

func TestKVStore_Equal(t *testing.T) {
	tests := []struct {
		desc         string
		entries      []kvPairs
		otherEntries []kvPairs
		equal        bool
	}{
		{
			desc:    "successful: clone empty store",
			entries: []kvPairs{},
			equal:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			for _, entry := range tt.entries {
				c.Set(entry.key, entry.value)
			}
			d := kvstore.NewKVStore()
			for _, entry := range tt.otherEntries {
				d.Set(entry.key, entry.value)
			}
			if eq, err := c.Equal(d); eq != tt.equal || err != nil {
				t.Errorf("KVStore.Equal() = %v, want %v", eq, tt.equal)
			}
		})
	}
}

func TestKVStore_Get(t *testing.T) {
	type args struct {
		key kvstore.Key
	}
	tests := []struct {
		desc    string
		args    args
		want    kvstore.Value
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			got, err := c.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("KVStore.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KVStore.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKVStore_GetPrefix(t *testing.T) {
	tests := []struct {
		desc    string
		args    args
		want    []kvstore.Value
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			got, err := c.GetPrefix(tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("KVStore.GetPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KVStore.GetPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKVStore_Has(t *testing.T) {
	tests := []struct {
		desc    string
		args    args
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := &KVStore{
				mu: sync.RWMutex{},
				m:  make(map[string][]byte, 0),
			}
			got, err := c.Has(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("KVStore.Has() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("KVStore.Has() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKVStore_Set(t *testing.T) {
	type args struct {
		key   kvstore.Key
		value kvstore.Value
	}
	tests := []struct {
		desc    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if err := c.Set(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("KVStore.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKVStore_Delete(t *testing.T) {
	type args struct {
		key kvstore.Key
	}
	tests := []struct {
		desc    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if err := c.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("KVStore.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKVStore_DeletePrefix(t *testing.T) {
	type args struct {
		prefix kvstore.KeyPrefix
	}
	tests := []struct {
		desc    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if err := c.DeletePrefix(tt.args.prefix); (err != nil) != tt.wantErr {
				t.Errorf("KVStore.DeletePrefix() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKVStore_ClearAll(t *testing.T) {
	tests := []struct {
		desc    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if err := c.ClearAll(); (err != nil) != tt.wantErr {
				t.Errorf("KVStore.ClearAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKVStore_Iterate(t *testing.T) {
	type args struct {
		prefix    kvstore.KeyPrefix
		consumer  kvstore.IteratorConsumerFn
		direction []kvstore.IterDirection
	}
	tests := []struct {
		desc    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if err := c.Iterate(tt.args.prefix, tt.args.consumer, tt.args.direction...); (err != nil) != tt.wantErr {
				t.Errorf("KVStore.Iterate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKVStore_IterateKeys(t *testing.T) {
	type args struct {
		prefix    kvstore.KeyPrefix
		consumer  kvstore.IteratorConsumerFn
		direction []kvstore.IterDirection
	}
	tests := []struct {
		desc    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if err := c.IterateKeys(tt.args.prefix, tt.args.consumer, tt.args.direction...); (err != nil) != tt.wantErr {
				t.Errorf("KVStore.IterateKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKVStore_Len(t *testing.T) {
	tests := []struct {
		desc string
		want int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if got := c.Len(); got != tt.want {
				t.Errorf("KVStore.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKVStore_Clone(t *testing.T) {
	tests := []struct {
		desc    string
		entries []kvPairs
		want    kvstore.KVStore
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := kvstore.NewKVStore()
			if got := c.Clone(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KVStore.Clone() = %v, want %v", got, tt.want)
			}
		})
	}
}
