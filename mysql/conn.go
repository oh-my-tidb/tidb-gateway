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

// Conn wraps net.Conn for data read/write.
// MySQL Packets: https://dev.mysql.com/doc/internals/en/mysql-packet.html
type Conn struct {
	conn        net.Conn
	r           *bufio.Reader
	w           *bufio.Writer
	sequence    uint8
	readTimeout time.Duration
	// maxAllowedPacket is the maximum size of one packet in readPacket.
	maxAllowedPacket uint64
	// accumulatedLength count the length of totally received 'payload' in readPacket.
	accumulatedLength uint64
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn: conn,
		r:    bufio.NewReaderSize(conn, defaultReaderSize),
		w:    bufio.NewWriterSize(conn, defaultWriterSize),
		// TODO: config max allowed packet
		maxAllowedPacket: math.MaxUint64,
	}
}

func (p *Conn) SetReadTimeout(timeout time.Duration) {
	p.readTimeout = timeout
}

func (p *Conn) SetMaxAllowedPacket(maxAllowedPacket uint64) {
	p.maxAllowedPacket = maxAllowedPacket
}

type Packet interface {
	Write(b *Buffer)
	Read(b *Buffer) error
}

func (p *Conn) SendPacket(pkt Packet) error {
	b := newBuffer(make([]byte, 4))
	pkt.Write(b)
	if err := p.writePacket(b.Bytes()); err != nil {
		return err
	}
	return p.Flush()
}

func (p *Conn) RecvPacket(pkg Packet) error {
	data, err := p.readPacket()
	if err != nil {
		return err
	}

	return pkg.Read(newBuffer(data))
}

func (p *Conn) readFull(data []byte) error {
	if p.readTimeout > 0 {
		if err := p.conn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return errors.WithStack(err)
		}
	}
	if _, err := io.ReadFull(p.r, data); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (p *Conn) readPacket() ([]byte, error) {
	var header [4]byte
	if err := p.readFull(header[:]); err != nil {
		return nil, err
	}

	sequence := header[3]
	if sequence != p.sequence {
		return nil, errors.Errorf("invalid sequence %d != %d", sequence, p.sequence)
	}

	p.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	// Accumulated payload length exceeds the limit.
	if p.accumulatedLength += uint64(length); p.accumulatedLength > p.maxAllowedPacket {
		return nil, errNetPacketTooLarge
	}

	data := make([]byte, length)
	if err := p.readFull(data); err != nil {
		return nil, err
	}
	return data, nil
}

// writePacket writes data that already have header
func (p *Conn) writePacket(data []byte) error {
	length := len(data) - 4
	for length >= MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.sequence

		if n, err := p.w.Write(data[:4+MaxPayloadLen]); err != nil {
			return errors.WithStack(ErrBadConn)
		} else if n != (4 + MaxPayloadLen) {
			return errors.WithStack(ErrBadConn)
		} else {
			p.sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.sequence

	if n, err := p.w.Write(data); err != nil {
		return errors.WithStack(ErrBadConn)
	} else if n != len(data) {
		return errors.WithStack(ErrBadConn)
	} else {
		p.sequence++
		return nil
	}
}

func (p *Conn) Flush() error {
	err := p.w.Flush()
	if err != nil {
		return errors.WithStack(err)
	}
	return err
}

func (p *Conn) RawConn() net.Conn {
	return p.conn
}

func (p *Conn) Close() {
	p.conn.Close()
}
