// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

package mysql

import (
	"bufio"
	"bytes"
	"io"
	"math"
	"net"
	"time"

	"github.com/pkg/errors"
)

// Portable analogs of some common call errors.
var (
	ErrBadConn           = errors.New("connection was bad")
	errNetPacketTooLarge = errors.New("net packet too large")
	ErrMalformPacket     = errors.New("malform packet")
)

const (
	defaultWriterSize = 16 * 1024
	defaultReaderSize = 16 * 1024
)

const (
	// MaxPayloadLen is the max packet payload length.
	MaxPayloadLen = 1<<24 - 1
)

// Options to determine when the sequence number should be reset.
const (
	SeqResetNone    uint8 = 0
	SeqResetOnRead  uint8 = 1
	SeqResetOnWrite uint8 = 2
	SeqResetBoth    uint8 = 3
)

// WriterFlusher represents a buffered writer. (like bufio.Writer)
type WriteFlusher interface {
	io.Writer
	Flush() error
}

// Conn wraps net.Conn for data read/write.
// MySQL Packets: https://dev.mysql.com/doc/internals/en/mysql-packet.html
type Conn struct {
	conn        net.Conn
	r           io.Reader
	w           WriteFlusher
	sequence    uint8
	seqreset    uint8
	readTimeout time.Duration
	// maxAllowedPacket is the maximum size of one packet in readPacket.
	maxAllowedPacket uint64
	compressor       *Compressor
}

// NewConn wraps a raw net.Conn into a Conn.
func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn: conn,
		r:    bufio.NewReaderSize(conn, defaultReaderSize),
		w:    bufio.NewWriterSize(conn, defaultWriterSize),
		// TODO: config max allowed packet
		maxAllowedPacket: math.MaxUint64,
	}
}

// SetRawConn resets the underlying net.Conn.
// Used for upgrading to TLS.
func (c *Conn) SetRawConn(conn net.Conn) {
	c.conn = conn
	c.r = bufio.NewReaderSize(conn, defaultReaderSize)
	c.w = bufio.NewWriterSize(conn, defaultWriterSize)
}

// SetReadTimeout sets the read timeout for the connection.
func (c *Conn) SetReadTimeout(timeout time.Duration) {
	c.readTimeout = timeout
}

// SetMaxAllowedPacket sets the maximum packet size.
func (c *Conn) SetMaxAllowedPacket(maxAllowedPacket uint64) {
	c.maxAllowedPacket = maxAllowedPacket
}

// Packet is the interface for a MySQL packet.
type Packet interface {
	Write(b *Buffer)
	Read(b *Buffer) error
}

// SendPacket sends a MySQL packet.
func (c *Conn) SendPacket(pkt Packet) error {
	b := newBuffer(nil)
	pkt.Write(b)
	if err := c.WritePacket(b.Bytes()); err != nil {
		return err
	}
	return c.Flush()
}

// RecvPacket receives a MySQL packet.
func (c *Conn) RecvPacket(pkg Packet) error {
	var b bytes.Buffer
	err := c.ReadPacket(&b)
	if err != nil {
		return err
	}

	return pkg.Read(newBuffer(b.Bytes()))
}

func (c *Conn) readFull(data []byte) error {
	if c.readTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
			return errors.WithStack(err)
		}
	}
	if _, err := io.ReadFull(c.r, data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ReadPacket reads a complete MySQL packet.
func (c *Conn) ReadPacket(b *bytes.Buffer) error {
	for {
		n, err := c.ReadPartialPacket(b)
		if err != nil {
			return err
		}
		if n < MaxPayloadLen {
			return nil
		}
	}
}

// ReadpartialPacket reads a MySQL wire packet. It may be
// part of a larger packet.
func (c *Conn) ReadPartialPacket(b *bytes.Buffer) (n int, err error) {
	var head [4]byte
	if err = c.readFull(head[:]); err != nil {
		return
	}
	if c.seqreset&SeqResetOnRead != 0 {
		c.seqreset &= ^SeqResetOnRead
		c.sequence = 0
	}
	sequence := head[3]
	if sequence != c.sequence {
		return 0, errors.Errorf("invalid sequence %d != %d", sequence, c.sequence)
	}
	c.sequence++

	n = readLen3(head[:3])
	b.Grow(n)
	readLen, err := b.ReadFrom(&io.LimitedReader{R: c.r, N: int64(n)})
	if int(readLen) != n {
		return int(readLen), err
	}
	return n, nil
}

// WritePacket writes data.
func (c *Conn) WritePacket(data []byte) error {
	if c.seqreset&SeqResetOnWrite != 0 {
		c.seqreset &= ^SeqResetOnWrite
		c.sequence = 0
	}

	var head [4]byte
	for {
		plen := len(data)
		if plen >= MaxPayloadLen {
			plen = MaxPayloadLen
		}
		writeLen3(head[:3], plen)
		head[3] = c.sequence

		n, err := c.w.Write(head[:])
		if err != nil || n != len(head) {
			return errors.WithStack(ErrBadConn)
		}

		if n, err := c.w.Write(data[:plen]); err != nil {
			return errors.WithStack(ErrBadConn)
		} else if n != plen {
			return errors.WithStack(ErrBadConn)
		} else {
			c.sequence++
			data = data[plen:]
		}

		if len(data) == 0 {
			return nil
		}
	}
}

// Flush flushes data to the underlying connection.
func (c *Conn) Flush() error {
	err := c.w.Flush()
	if err != nil {
		return errors.WithStack(err)
	}
	return err
}

// RawConn returns the underlying net.Conn.
func (p *Conn) RawConn() net.Conn {
	return p.conn
}

// Close closes the connection.
func (p *Conn) Close() {
	p.conn.Close()
}

// SetResetOption marks the connection to reset sequence on next read/write.
func (c *Conn) SetResetOption(opt uint8) {
	c.seqreset = opt
	if c.compressor != nil {
		c.compressor.SetResetOption(opt)
	}
}

// EnableCompression wraps the underlying reader and writer to support compression.
func (c *Conn) EnableCompression() {
	c.compressor = NewCompressor(c.r, c.w)
	c.r = c.compressor
	c.w = c.compressor
}
