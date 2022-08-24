package mysql

import (
	"bytes"
	"compress/zlib"
	"io"

	"github.com/pkg/errors"
)

const (
	minCompressLen = 128
	maxBufferLen   = (1 << 23) - 1
)

// Compressor wraps a Reader and a WriteFlusher for compression.
type Compressor struct {
	r           io.Reader
	w           WriteFlusher
	sequence    uint8
	seqreset    uint8
	readBuffer  bytes.Buffer // decompressed data to be read.
	writeBuffer bytes.Buffer // bytes to be compressed.
	flushBuffer bytes.Buffer // compressed data to be sent.
}

// NewCompressor creates a new Compressor.
func NewCompressor(r io.Reader, w WriteFlusher) *Compressor {
	return &Compressor{
		r: r,
		w: w,
	}
}

// Read reads data from the underlying reader.
func (c *Compressor) Read(p []byte) (int, error) {
	// drain buffer before reading next trunk.
	if n := c.readBuffer.Len(); n > 0 {
		if n > len(p) {
			n = len(p)
		}
		return c.readBuffer.Read(p[:n])
	}
	return 0, c.loadNextTrunk()
}

func (c *Compressor) loadNextTrunk() error {
	c.readBuffer.Reset()

	var head [7]byte
	n, err := io.ReadFull(c.r, head[:])
	if n != 7 {
		return err // err is guranateed not nil.
	}
	if c.seqreset&SeqResetOnRead != 0 {
		c.seqreset &= ^SeqResetOnRead
		c.sequence = 0
	}

	payloadLen := readLen3(head[0:3])
	sequence := head[3]
	uncompressedLen := readLen3(head[4:7])

	if sequence != c.sequence {
		return errors.Errorf("invalid sequence %d != %d", sequence, c.sequence)
	}

	if uncompressedLen == 0 {
		// uncompressed payload.
		n, err := io.CopyN(&c.readBuffer, c.r, int64(payloadLen))
		if n != int64(payloadLen) {
			return err // err is guranateed not nil.
		}
	} else {
		zr, err := zlib.NewReader(io.LimitReader(c.r, int64(payloadLen)))
		if err != nil {
			return err
		}
		n, err := io.Copy(&c.readBuffer, zr)
		if n != int64(uncompressedLen) {
			return errors.Errorf("uncompessed length mismatch %d != %d", n, uncompressedLen)
		}
	}
	c.sequence++
	return nil
}

// Write writes data to the underlying writer. It works like bufio.Writer with compression.
func (c *Compressor) Write(p []byte) (int, error) {
	for len(p) > 0 {
		capacity := maxBufferLen - c.writeBuffer.Len()
		if capacity >= len(p) {
			return c.writeBuffer.Write(p)
		}
		n, err := c.writeBuffer.Write(p[:capacity])
		if n != capacity {
			return n, err
		}
		err = c.Flush()
		if err != nil {
			return n, err
		}
		p = p[capacity:]
	}
	return 0, nil
}

// Flush compress then flush the data to the underlying writer.
func (c *Compressor) Flush() error {
	if c.seqreset&SeqResetOnWrite != 0 {
		c.seqreset &= ^SeqResetOnWrite
		c.sequence = 0
	}

	var head [7]byte
	var payload []byte

	if c.writeBuffer.Len() < minCompressLen {
		// write without compression.
		writeLen3(head[0:3], c.writeBuffer.Len())
		head[3] = c.sequence
		writeLen3(head[4:7], 0)
		payload = c.writeBuffer.Bytes()
	} else {
		// with compression.
		zw := zlib.NewWriter(&c.flushBuffer)
		n, err := zw.Write(c.writeBuffer.Bytes())
		if n != c.writeBuffer.Len() {
			return err // err is guranateed not nil.
		}
		err = zw.Close()
		if err != nil {
			return err
		}
		writeLen3(head[0:3], c.flushBuffer.Len())
		head[3] = c.sequence
		writeLen3(head[4:7], c.writeBuffer.Len())
		payload = c.flushBuffer.Bytes()
	}

	n, _ := c.w.Write(head[:])
	if n != 7 {
		return errors.WithStack(ErrBadConn)
	}
	n, _ = c.w.Write(payload)
	if n != len(payload) {
		return errors.WithStack(ErrBadConn)
	}
	c.sequence++
	c.writeBuffer.Reset()
	c.flushBuffer.Reset()
	return c.w.Flush()
}

// SetResetOption marks the sequence to be reset on next read or write.
func (c *Compressor) SetResetOption(opt uint8) {
	c.seqreset = opt
}
