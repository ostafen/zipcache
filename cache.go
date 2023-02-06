package zipcache

import (
	"bytes"
	"compress/flate"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

type chunk struct {
	isCompressed bool
	data         []byte
}

var (
	ErrKeyBound = errors.New("key already bound")
)

type Writer interface {
	io.WriteCloser
	Flush() error
}

type Reader interface {
	io.ReadCloser
}

type WriterFactory func(w io.Writer) (Writer, error)
type ReaderFactory func(w io.Reader) (Reader, error)

func flateWriter(w io.Writer) (Writer, error) {
	return flate.NewWriter(w, flate.BestSpeed)
}

func flateReader(r io.Reader) (Reader, error) {
	return flate.NewReader(r), nil
}

type Config struct {
	ChunkSize    int
	ChunkMinGain float64
	NewWriter    WriterFactory
	NewReader    ReaderFactory
}

const (
	chunkSizeDefault     = 4096
	chunkMinRatioDefault = 0.05
)

func DefaultConfig() Config {
	return Config{
		ChunkSize:    chunkSizeDefault,
		ChunkMinGain: chunkMinRatioDefault,
		NewWriter:    flateWriter,
		NewReader:    flateReader,
	}
}

func (cfg Config) WithChunkSize(size int) Config {
	cfg.ChunkSize = size
	return cfg
}

func (cfg Config) WithChunkMinGain(gain float64) Config {
	cfg.ChunkMinGain = gain
	return cfg
}

func (cfg Config) WithReaderWriter(r ReaderFactory, w WriterFactory) Config {
	cfg.NewReader = r
	cfg.NewWriter = w
	return cfg
}

type ZipCache struct {
	cfg Config

	mtx sync.RWMutex
	m   map[string]pointer

	chunks          []*atomic.Pointer[chunk]
	nChunks         atomic.Int32
	currChunkOffset uint32
}

type pointer struct {
	blockNumber uint32
	offset      uint32
	size        uint32
}

func (p pointer) Block() int {
	return int(p.blockNumber)
}

func (p pointer) Offset() int {
	return int(p.offset)
}

func (p pointer) Len() int {
	return int(p.size)
}

func (c *ZipCache) newChunk() *chunk {
	return &chunk{
		isCompressed: false,
		data:         make([]byte, c.cfg.ChunkSize),
	}
}

func New(cfg Config) *ZipCache {
	c := &ZipCache{
		cfg:    cfg,
		m:      map[string]pointer{},
		chunks: make([]*atomic.Pointer[chunk], 0),
	}
	c.chunks = append(c.chunks, &atomic.Pointer[chunk]{})
	c.chunks[0].Store(c.newChunk())
	c.nChunks.Store(1)
	return c
}

func (c *ZipCache) compressChunks(ptrs []*atomic.Pointer[chunk]) error {
	for _, ptr := range ptrs {
		var buf bytes.Buffer

		w, err := c.cfg.NewWriter(&buf)
		if err != nil {
			return err
		}

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

		gain := 1 - (float64(buf.Len()) / float64(len(b.data)))
		if gain >= c.cfg.ChunkMinGain {
			newBlock.data = buf.Bytes()
		}

		ptr.Store(newBlock)
	}

	return nil
}

// TODO: do not create reader multiple times
func (c *ZipCache) uncompress(src, dst []byte) (int, error) {
	r, err := c.cfg.NewReader(bytes.NewBuffer(src))
	if err != nil {
		return -1, err
	}
	defer r.Close()
	return r.Read(dst)
}

func newPointer(blockNumber, byteOffset, len uint64) pointer {
	return pointer{
		size:        uint32(len),
		offset:      uint32(byteOffset),
		blockNumber: uint32(blockNumber),
	}
}

func (c *ZipCache) Put(k, v []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, has := c.m[string(k)]; has {
		return ErrKeyBound
	}

	c.m[string(k)] = newPointer(uint64(uint32(len(c.chunks)-1)), uint64(c.currChunkOffset), uint64(len(v)))

	compressChunks := make([]*atomic.Pointer[chunk], 0)

	size := len(v)
	for size > 0 {
		currChunkPtr := c.chunks[len(c.chunks)-1]
		currChunk := currChunkPtr.Load()

		n := copy(currChunk.data[c.currChunkOffset:], v[len(v)-size:])
		c.currChunkOffset += uint32(n)

		if c.currChunkOffset == uint32(c.cfg.ChunkSize) {
			compressChunks = append(compressChunks, currChunkPtr)

			var ptr atomic.Pointer[chunk]
			ck := c.newChunk()
			ptr.Store(ck)

			c.chunks = append(c.chunks, &ptr)
			c.currChunkOffset = 0

			c.nChunks.Add(1)
		}
		size -= int(n)
	}

	if len(compressChunks) > 0 {
		go c.compressChunks(compressChunks)
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

	nChunks := 1 + (ptr.Len()+(c.cfg.ChunkSize-1))/c.cfg.ChunkSize
	chunks := make([]*chunk, 0, nChunks)
	for i := 0; i < nChunks; i++ {
		if i+ptr.Block() < len(c.chunks) {
			chunks = append(chunks, c.chunks[i+ptr.Block()].Load())
		}
	}
	c.mtx.RUnlock()

	dst := make([]byte, ptr.Len())
	off := ptr.Offset()
	n := 0
	for _, chunk := range chunks {
		uncompressed := chunk.data

		if chunk.isCompressed {
			uncompressed = make([]byte, c.cfg.ChunkSize)
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
