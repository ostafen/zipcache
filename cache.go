package zipcache

import (
	"bytes"
	"compress/gzip"
	"errors"
	"sync"
	"sync/atomic"
)

const (
	chunkSizeDefault = 4096 * 4
)

type chunk struct {
	isCompressed bool
	data         []byte
}

var (
	ErrKeyExist = errors.New("key already exists")
)

type ZipCache struct {
	isCompressing atomic.Bool

	mtx sync.RWMutex
	m   map[string]pointer

	chunks          []*atomic.Pointer[chunk]
	nChunks         atomic.Int32
	currChunkOffset uint32
}

type pointer uint64

func (p pointer) Block() int {
	return int(p >> 32)
}

func (p pointer) Offset() int {
	return int(uint32(p) >> 16)
}

func (p pointer) Len() int {
	return int(uint16(p))
}

func newChunk() *chunk {
	return &chunk{
		isCompressed: false,
		data:         make([]byte, chunkSizeDefault),
	}
}

func New() *ZipCache {
	c := &ZipCache{
		isCompressing: atomic.Bool{},
		m:             map[string]pointer{},
		chunks:        make([]*atomic.Pointer[chunk], 0),
	}
	c.chunks = append(c.chunks, &atomic.Pointer[chunk]{})
	c.chunks[0].Store(newChunk())
	c.nChunks.Store(1)
	return c
}

func (c *ZipCache) compressBlock(ptrs []*atomic.Pointer[chunk]) error {
	for _, ptr := range ptrs {
		var buf bytes.Buffer

		w := gzip.NewWriter(&buf)

		b := ptr.Load()

		if _, err := w.Write(b.data); err != nil {
			return err
		}

		if err := w.Flush(); err != nil {
			return err
		}

		if err := w.Close(); err != nil {
			return err
		}

		newBlock := &chunk{
			isCompressed: buf.Len() < len(b.data),
			data:         b.data,
		}

		if buf.Len() < len(b.data) { // TODO: check if compressed data saves at least some percentage of space
			newBlock.data = buf.Bytes()
		}

		ptr.Store(newBlock)
	}

	c.isCompressing.Store(false)
	return nil
}

func (c *ZipCache) uncompress(src, dst []byte) (int, error) {
	r, _ := gzip.NewReader(bytes.NewReader(src))
	return r.Read(dst)
}

func newPointer(blockOffset, byteOffset, len uint64) pointer {
	return pointer((blockOffset << 32) + (byteOffset << 16) + len)
}

func (c *ZipCache) Put(k, v []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, has := c.m[string(k)]; has {
		return ErrKeyExist
	}

	c.m[string(k)] = newPointer(uint64(uint32(len(c.chunks)-1)), uint64(c.currChunkOffset), uint64(len(v)))

	compressChunks := make([]*atomic.Pointer[chunk], 0)

	size := len(v)
	for size > 0 {
		currChunkPtr := c.chunks[len(c.chunks)-1]
		currChunk := currChunkPtr.Load()

		n := copy(currChunk.data[c.currChunkOffset:], v[len(v)-size:])
		c.currChunkOffset += uint32(n)

		if c.currChunkOffset == chunkSizeDefault {
			compressChunks = append(compressChunks, currChunkPtr)

			var ptr atomic.Pointer[chunk]
			ck := newChunk()
			ptr.Store(ck)

			c.chunks = append(c.chunks, &ptr)
			c.currChunkOffset = 0

			c.nChunks.Add(1)
		}
		size -= int(n)
	}

	if len(compressChunks) > 0 {
		go c.compressBlock(compressChunks)
	}
	return nil
}

func (c *ZipCache) Get(k []byte) ([]byte, error) {
	c.mtx.RLock()

	ptr, ok := c.m[string(k)]
	if !ok {
		c.mtx.RUnlock()
		return nil, nil
	}

	currChunk := c.chunks[ptr.Block()].Load()
	if !currChunk.isCompressed && (ptr.Offset()+ptr.Len() <= len(currChunk.data)) {
		c.mtx.RUnlock()
		return currChunk.data[ptr.Offset() : ptr.Offset()+ptr.Len()], nil
	}

	nChunks := 1 + (ptr.Len()+(chunkSizeDefault-1))/chunkSizeDefault
	chunks := make([]*chunk, 0, nChunks)
	for i := 0; i < nChunks; i++ {
		chunks = append(chunks, c.chunks[i+ptr.Block()].Load())
	}
	c.mtx.RUnlock()

	dst := make([]byte, ptr.Len())
	off := ptr.Offset()
	n := 0
	for _, chunk := range chunks {
		uncompressed := chunk.data

		if chunk.isCompressed {
			uncompressed = make([]byte, chunkSizeDefault)
			_, err := c.uncompress(chunk.data, uncompressed)
			if err != nil {
				return nil, err
			}
		}

		endOff := off + ptr.Len() - n
		if endOff > len(uncompressed) {
			endOff = len(uncompressed)
		}

		copied := copy(dst[n:], uncompressed[off:endOff])
		n += copied
		off = 0

		if n == ptr.Len() {
			break
		}
	}

	return dst, nil
}

func (c *ZipCache) Size() int64 {
	c.mtx.RLock()

	var size int64
	for _, ck := range c.chunks {
		size += int64(len(ck.Load().data))
	}

	c.mtx.RUnlock()

	return size
}
