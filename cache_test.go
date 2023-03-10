package zipcache

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingleThread(t *testing.T) {
	cache := New(DefaultConfig().WithChunkSize(100))

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	rand.Seed(time.Now().Unix())

	n := 1000
	for i := 0; i < n; i++ {
		k := make([]byte, 8)
		v := make([]byte, 10+rand.Int()%256+1)

		rand.Read(k)
		rand.Read(v)

		for j := 0; j < len(v); j++ {
			v[j] %= 10
		}

		err := cache.Put(k, v)
		if errors.Is(err, ErrKeyBound) {
			continue
		}
		require.NoError(t, err)

		keys = append(keys, k)
		values = append(values, v)
	}

	for i := 0; i < len(keys); i++ {
		v, err := cache.Get(keys[i])
		require.NoError(t, err)
		require.Equal(t, v, values[i])
	}
}
