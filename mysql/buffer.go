package mysql

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

// Buffer wraps bytes.Buffer for read/write mysql data types.
type Buffer struct {
	b *bytes.Buffer
}

func newBuffer(data []byte) *Buffer {
	return &Buffer{bytes.NewBuffer(data)}
}

// WriteByte writes a single byte.
func (b *Buffer) WriteByte(by byte) {
	b.b.WriteByte(by)
}

// ReadByte reads a single byte.
func (b *Buffer) ReadByte() (byte, error) {
	by, err := b.b.ReadByte()
	return by, errors.WithStack(err)
}

// WriteBytes writes bytes.
func (b *Buffer) WriteBytes(bys []byte) {
	b.b.Write(bys)
}

// ReadBytes reads n bytes.
func (b *Buffer) ReadBytes(n int) ([]byte, error) {
	data := b.b.Next(n)
	if len(data) == n {
		return data, nil
	}
	return nil, errors.WithStack(io.EOF)
}

// WriteString writes a string followed by a null byte.
func (b *Buffer) WriteStringNull(s string) {
	b.b.WriteString(s)
	b.WriteByte(0x00)
}

// ReadStringNull reads a string followed by a null byte.
func (b *Buffer) ReadStringNull() (string, error) {
	s, err := b.b.ReadString(0x00)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return s[:len(s)-1], nil
}

// WriteUint32 writes a uint32.
func (b *Buffer) WriteUint32(n uint32) {
	var b4 [4]byte
	binary.LittleEndian.PutUint32(b4[:], n)
	b.WriteBytes(b4[:])
}

// ReadUint32 reads a uint32.
func (b *Buffer) ReadUint32() (uint32, error) {
	data, err := b.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// WriteUint16 writes a uint16.
func (b *Buffer) WriteUint16(n uint16) {
	var b2 [2]byte
	binary.LittleEndian.PutUint16(b2[:], n)
	b.WriteBytes(b2[:])
}

// ReadUint16 reads a uint16.
func (b *Buffer) ReadUint16() (uint16, error) {
	data, err := b.ReadBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(data), nil
}

// WriteUint24 writes a uint24.
func (b *Buffer) WriteUint24(n uint32) {
	b.WriteUint16(uint16(n & 0xFFFF))
	b.WriteByte(byte(n >> 16))
}

// ReadUint24 reads a uint24.
func (b *Buffer) ReadUint24() (uint32, error) {
	u16, err := b.ReadUint16()
	if err != nil {
		return 0, err
	}
	b3, err := b.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint32(u16) | uint32(b3)<<16, nil
}

// WriteUint64 writes a uint64.
func (b *Buffer) WriteUint64(n uint64) {
	var b8 [8]byte
	binary.LittleEndian.PutUint64(b8[:], n)
	b.WriteBytes(b8[:])
}

// ReadUint64 reads a uint64.
func (b *Buffer) ReadUint64() (uint64, error) {
	data, err := b.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data), nil
}

// Len returns the number of bytes written to the buffer or left to read from
// the buffer
func (b *Buffer) Len() int {
	return b.b.Len()
}

// Skip skips n bytes for read.
func (b *Buffer) Skip(n int) error {
	_, err := b.ReadBytes(n)
	return err
}

// WriteLenencInt writes a lenenc int.
func (b *Buffer) WriteLenencInt(n uint64) {
	switch {
	case n < 251:
		b.WriteByte(byte(n))
	case n >= 251 && n < (1<<16):
		b.WriteByte(0xFC)
		b.WriteUint16(uint16(n))
	case n >= (1<<16) && n < (1<<24):
		b.WriteByte(0xFD)
		b.WriteUint24(uint32(n))
	default:
		b.WriteByte(0xFE)
		b.WriteUint64(n)
	}
}

// ReadLenencInt reads a lenenc int.
func (b *Buffer) ReadLenencInt() (uint64, error) {
	b1, err := b.ReadByte()
	if err != nil {
		return 0, err
	}
	switch {
	case b1 < 0xFC:
		return uint64(b1), nil
	case b1 == 0xFC:
		n, err := b.ReadUint16()
		return uint64(n), err
	case b1 == 0xFD:
		n, err := b.ReadUint24()
		return uint64(n), err
	case b1 == 0xFE:
		return b.ReadUint64()
	}
	return 0, errors.New("invalid lenenc int")
}

// WriteLenencString writes a lenenc string.
func (b *Buffer) WriteLenencString(s string) {
	b.WriteLenencInt(uint64(len(s)))
	b.b.WriteString(s)
}

// ReadLenencString reads a lenenc string.
func (b *Buffer) ReadLenencString() (string, error) {
	n, err := b.ReadLenencInt()
	if err != nil {
		return "", err
	}
	data, err := b.ReadBytes(int(n))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Bytes returns the underlying bytes.
func (b *Buffer) Bytes() []byte {
	return b.b.Bytes()
}
