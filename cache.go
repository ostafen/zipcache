package zipcache

import (
	"bytes"
	"compress/flate"
	"errors"
	"sync"
	"sync/atomic"
)

const (
	blockSizeDefault        = 4096
	compressionLevelDefault = 1
)

type Block struct {
	size         int
	isCompressed bool
	data         []byte
}

var ErrCompressionRunning = errors.New("already running compression")

type CompressedCache struct {
	isCompressing atomic.Bool

	mtx sync.RWMutex
	m   map[string]pointer

	blocks []atomic.Pointer[Block]

	currBlockOffset uint32
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

func newBlock() *Block {
	return &Block{
		size:         0,
		isCompressed: false,
		data:         make([]byte, blockSizeDefault),
	}
}

func New() *CompressedCache {
	c := &CompressedCache{
		isCompressing: atomic.Bool{},
		m:             map[string]pointer{},
		blocks:        make([]atomic.Pointer[Block], 1),
	}
	c.blocks[0].Store(newBlock())
	return c
}

func (c *CompressedCache) compressBlock(ptr atomic.Pointer[Block]) error {
	var buf bytes.Buffer

	w, err := flate.NewWriter(&buf, compressionLevelDefault)
	if err != nil {
		return err
	}

	b := ptr.Load()
	if _, err := w.Write(b.data[:c.currBlockOffset]); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	newBlock := &Block{
		isCompressed: buf.Len() < len(b.data),
		data:         b.data,
	}

	if buf.Len() < len(b.data) { // TODO: check if compressed data saves at least some percentage of space
		newBlock.data = buf.Bytes()
	}

	ptr.Store(newBlock)
	c.isCompressing.Store(false)
	return err
}

func (c *CompressedCache) uncompress(src, dst []byte) (int, error) {
	r := flate.NewReader(bytes.NewReader(src))
	return r.Read(dst)
}

func newPointer(blockOffset, byteOffset, len uint64) pointer {
	return pointer((blockOffset << 32) + (byteOffset << 16) + len)
}

func (c *CompressedCache) Put(k, v []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	currBlockPtr := c.blocks[len(c.blocks)-1]
	if c.currBlockOffset+uint32(len(v)) > blockSizeDefault {
		if !c.isCompressing.CompareAndSwap(false, true) {
			return ErrCompressionRunning
		}

		prevBlockPtr := currBlockPtr
		currBlock := &Block{
			size:         0,
			isCompressed: false,
			data:         make([]byte, blockSizeDefault),
		}
		var ptr atomic.Pointer[Block]
		ptr.Store(currBlock)

		c.blocks = append(c.blocks, ptr)
		c.currBlockOffset = 0

		go c.compressBlock(prevBlockPtr)
	}

	newOffset := c.currBlockOffset + uint32(len(v))

	currBlock := currBlockPtr.Load()
	copy(currBlock.data[c.currBlockOffset:], v)

	blockOffset := uint64(uint32(len(c.blocks) - 1))
	byteOffset := uint64(c.currBlockOffset)
	len := uint64(len(v))

	c.m[string(k)] = newPointer(blockOffset, byteOffset, len)

	c.currBlockOffset = newOffset

	return nil
}

func (c *CompressedCache) Get(k []byte) ([]byte, error) {
	c.mtx.RLock()

	ptr, ok := c.m[string(k)]
	if !ok {
		c.mtx.RUnlock()
		return nil, nil
	}

	bb := c.blocks[ptr.Block()].Load()
	c.mtx.RUnlock()

	data := bb.data
	if bb.isCompressed {
		dst := make([]byte, blockSizeDefault) // keep this separatedly?
		_, err := c.uncompress(bb.data, dst)
		if err != nil {
			return nil, err
		}
		data = dst
	}
	return data[ptr.Offset() : ptr.Offset()+ptr.Len()], nil
}
