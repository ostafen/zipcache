package zipcache

import (
	"bytes"
	"compress/gzip"
	"errors"
	"sync"
	"sync/atomic"
)

// put a value across multiple blocks

const (
	chunkSizeDefault        = 1024
	compressionLevelDefault = 1
)

type chunk struct {
	isCompressed bool
	data         []byte
}

var (
	ErrKeyExist = errors.New("key already exists")
)

type CompressedCache struct {
	isCompressing atomic.Bool

	mtx sync.RWMutex
	m   map[string]pointer

	blocks []*atomic.Pointer[chunk]

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

func newBlock() *chunk {
	return &chunk{
		isCompressed: false,
		data:         make([]byte, chunkSizeDefault),
	}
}

func New() *CompressedCache {
	c := &CompressedCache{
		isCompressing: atomic.Bool{},
		m:             map[string]pointer{},
		blocks:        make([]*atomic.Pointer[chunk], 0),
	}
	c.blocks = append(c.blocks, &atomic.Pointer[chunk]{})
	c.blocks[0].Store(newBlock())
	return c
}

func (c *CompressedCache) compressBlock(ptrs []*atomic.Pointer[chunk]) error {
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

func (c *CompressedCache) uncompress(src, dst []byte) (int, error) {
	r, _ := gzip.NewReader(bytes.NewReader(src))
	return r.Read(dst)
}

func newPointer(blockOffset, byteOffset, len uint64) pointer {
	return pointer((blockOffset << 32) + (byteOffset << 16) + len)
}

func (c *CompressedCache) Put(k, v []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, has := c.m[string(k)]; has {
		return ErrKeyExist
	}

	c.m[string(k)] = newPointer(uint64(uint32(len(c.blocks)-1)), uint64(c.currChunkOffset), uint64(len(v)))

	compressChunks := make([]*atomic.Pointer[chunk], 0)

	size := len(v)
	for size > 0 {
		currChunkPtr := c.blocks[len(c.blocks)-1]
		currChunk := currChunkPtr.Load()

		n := copy(currChunk.data[c.currChunkOffset:], v[len(v)-size:])
		c.currChunkOffset += uint32(n)

		if c.currChunkOffset == chunkSizeDefault {
			compressChunks = append(compressChunks, currChunkPtr)

			var ptr atomic.Pointer[chunk]
			bb := newBlock()
			ptr.Store(bb)

			c.blocks = append(c.blocks, &ptr)
			c.currChunkOffset = 0
		}
		size -= int(n)
	}

	if len(compressChunks) > 0 {
		go c.compressBlock(compressChunks)
	}
	return nil
}

func (c *CompressedCache) Get(k []byte) ([]byte, error) {
	c.mtx.RLock()

	ptr, ok := c.m[string(k)]
	if !ok {
		c.mtx.RUnlock()
		return nil, nil
	}

	currChunk := c.blocks[ptr.Block()].Load()
	if !currChunk.isCompressed && (ptr.Offset()+ptr.Len() <= len(currChunk.data)) {
		c.mtx.RUnlock()
		return currChunk.data[ptr.Offset() : ptr.Offset()+ptr.Len()], nil
	}

	nChunks := 1 + (ptr.Len()+(chunkSizeDefault-1))/chunkSizeDefault
	chunks := make([]*chunk, 0, nChunks)
	for i := 0; i < nChunks; i++ {
		chunks = append(chunks, c.blocks[i+ptr.Block()].Load())
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
			return uncompressed[ptr.Offset() : ptr.Offset()+ptr.Len()], nil
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
