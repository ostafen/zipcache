package zipcache

import (
	"bytes"
	"compress/flate"
)

const (
	blockSize               = 4096
	compressionLevelDefault = 1
)

type Block struct {
	size         int
	isCompressed bool
	data         []byte
}

type CompressedCache struct {
	m map[string]pointer

	blocks []*Block

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

func New() *CompressedCache {
	return &CompressedCache{
		m: map[string]pointer{},
		blocks: []*Block{
			{
				size:         0,
				isCompressed: false,
				data:         make([]byte, blockSize),
			},
		},
	}
}

func (c *CompressedCache) compress() error {
	var buf bytes.Buffer

	w, err := flate.NewWriter(&buf, compressionLevelDefault)
	if err != nil {
		return err
	}

	bb := c.blocks[len(c.blocks)-1]
	if _, err := w.Write(bb.data[:c.currBlockOffset]); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	if buf.Len() < len(bb.data) { // TODO: check if compressed data saves at least some percentage of space
		bb.data = buf.Bytes()
		bb.isCompressed = true
	}
	return err
}

func (c *CompressedCache) uncompress(src, dst []byte) (int, error) {
	r := flate.NewReader(bytes.NewReader(src))
	return r.Read(dst)
}

func (c *CompressedCache) Put(k, v []byte) error {
	// check len(v) <= maxuint16
	if c.currBlockOffset+uint32(len(v)) > blockSize {
		if err := c.compress(); err != nil {
			return err
		}
		c.blocks = append(c.blocks, &Block{
			size:         0,
			isCompressed: false,
			data:         make([]byte, blockSize),
		})
		c.currBlockOffset = 0
	}

	newOffset := c.currBlockOffset + uint32(len(v))

	bb := c.blocks[len(c.blocks)-1]
	copy(bb.data[c.currBlockOffset:], v)

	bOff := uint64(uint32(len(c.blocks) - 1))
	off := uint64(c.currBlockOffset)
	len := uint64(len(v))

	c.m[string(k)] = pointer((bOff << 32) + (off << 16) + len)

	c.currBlockOffset = newOffset

	return nil
}

func (c *CompressedCache) Get(k []byte) ([]byte, error) {
	ptr, ok := c.m[string(k)]
	if !ok {
		return nil, nil
	}

	bb := c.blocks[ptr.Block()]
	data := bb.data
	if bb.isCompressed {
		dst := make([]byte, blockSize) // keep this separatedly?
		_, err := c.uncompress(bb.data, dst)
		if err != nil {
			return nil, err
		}
		data = dst
	}
	return data[ptr.Offset() : ptr.Offset()+ptr.Len()], nil
}
