package zipcache

import (
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingleThread(t *testing.T) {
	cache := New()

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	rand.Seed(time.Now().Unix())
	for i := 0; i < 4*12000; i++ {
		k := make([]byte, 4)
		v := make([]byte, 4)

		rand.Read(k)
		rand.Read(v)

		for j := 0; j < len(v); j++ {
			v[j] %= 5
		}

		err := cache.Put(k, v)

		require.NoError(t, err)

		vs, err := cache.Get(k)
		require.NoError(t, err)
		require.Equal(t, v, vs)

		keys = append(keys, k)
		values = append(values, v)
	}

	total := time.Duration(0)
	for i := 0; i < len(keys); i++ {
		now := time.Now()
		v, err := cache.Get(keys[i])
		total += time.Since(now)
		require.NoError(t, err)
		require.Equal(t, v, values[i])
	}

	log.Println((total.Seconds() * 1000) / float64(len(keys)))

	size := 0
	for _, bb := range cache.blocks {
		size += len(bb.Load().data)
	}

	log.Println(float64(size) / (float64(len(cache.blocks) * chunkSizeDefault)))
}
