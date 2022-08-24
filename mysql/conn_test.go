package mysql

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnMultiplePackets(t *testing.T) {
	client, server := makeConnPair()
	testConnMultiplePackets(t, client, server)
	client, server = makeConnPairWithCompression()
	testConnMultiplePackets(t, client, server)
}

func testConnMultiplePackets(t *testing.T, client, server *Conn) {
	defer client.Close()
	defer server.Close()

	for i := 0; i < 10; i++ {
		p := randomPayloads()
		var wg sync.WaitGroup
		goSendPayloads(t, &wg, client, p)
		result := recvPayloads(t, server, len(p))
		require.Equal(t, p, result)
		wg.Wait()
	}
}

func TestConnRequestResponse(t *testing.T) {
	client, server := makeConnPair()
	testConnRequestResponse(t, client, server)
	client, server = makeConnPairWithCompression()
	testConnRequestResponse(t, client, server)
}

func testConnRequestResponse(t *testing.T, client, server *Conn) {
	defer client.Close()
	defer server.Close()

	for i := 0; i < 5; i++ {
		p := randomPayloads()
		var wg sync.WaitGroup
		goSendPayloads(t, &wg, client, p)
		result := recvPayloads(t, server, len(p))
		require.Equal(t, p, result)
		wg.Wait()

		p, result = randomPayloads(), nil
		goSendPayloads(t, &wg, server, p)
		result = recvPayloads(t, client, len(p))
		require.Equal(t, p, result)
		wg.Wait()

		// reset sequence number.
		client.SetResetOption(SeqResetOnWrite)
		server.SetResetOption(SeqResetOnRead)
	}
}

func randomPayloads() [][]byte {
	p := make([][]byte, rand.Intn(10)+1)
	for i := range p {
		p[i] = make([]byte, rand.Intn(10)*rand.Intn(1024))
		rand.Read(p[i])
	}
	return p
}

func goSendPayloads(t *testing.T, wg *sync.WaitGroup, conn *Conn, payload [][]byte) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, p := range payload {
			err := conn.WritePacket(p)
			require.NoError(t, err)
		}
		err := conn.Flush()
		require.NoError(t, err)
	}()
}

func recvPayloads(t *testing.T, conn *Conn, n int) [][]byte {
	var payload [][]byte
	for i := 0; i < n; i++ {
		var b bytes.Buffer
		err := conn.ReadPacket(&b)
		require.NoError(t, err)
		payload = append(payload, b.Bytes())
	}
	return payload
}

type mockConn struct {
	*io.PipeReader
	*io.PipeWriter
}

func (conn mockConn) Close() error {
	conn.PipeReader.Close()
	conn.PipeWriter.Close()
	return nil
}

func (conn mockConn) LocalAddr() net.Addr {
	return nil
}

func (conn mockConn) RemoteAddr() net.Addr {
	return nil
}

func (conn mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (conn mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (conn mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func makeConnPair() (*Conn, *Conn) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	return NewConn(mockConn{r1, w2}), NewConn(mockConn{r2, w1})
}

func makeConnPairWithCompression() (*Conn, *Conn) {
	conn1, conn2 := makeConnPair()
	conn1.EnableCompression()
	conn2.EnableCompression()
	return conn1, conn2
}
