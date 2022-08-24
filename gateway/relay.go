package gateway

import (
	"bytes"
	"io"

	"github.com/oh-my-tidb/tidb-gateway/mysql"
	"github.com/pkg/errors"
)

// RelayRawBytes relays raw bytes between remote and backend.
func RelayRawBytes(remote, backend *mysql.Conn, quit <-chan struct{}) error {
	remote.SetResetOption(mysql.SeqResetBoth)
	backend.SetResetOption(mysql.SeqResetBoth)
	errCh := make(chan error, 2) // nolint:gomnd // nolint
	go func() {
		_, err := io.Copy(backend.RawConn(), remote.RawConn())
		errCh <- errors.Wrap(err, "remote -> backend closed")
	}()
	go func() {
		_, err := io.Copy(remote.RawConn(), backend.RawConn())
		errCh <- errors.Wrap(err, "backend -> remote closed")
	}()
	select {
	case err := <-errCh:
		return err
	case <-quit:
		return errors.New("relayer is closed")
	}
}

// RelayPacketes relays packets between remote and backend.
func RelayPackets(remote, backend *mysql.Conn, quit <-chan struct{}) error {
	remote.SetResetOption(mysql.SeqResetBoth)
	backend.SetResetOption(mysql.SeqResetBoth)
	errCh := make(chan error, 2) // nolint:gomnd // nolint
	go copyInboundPackets(remote, backend, errCh)
	go copyOutboundPackets(remote, backend, errCh)
	select {
	case err := <-errCh:
		return err
	case <-quit:
		return errors.New("relayer is closed")
	}
}

func copyInboundPackets(remote, backend *mysql.Conn, errCh chan error) {
	var b bytes.Buffer
	for {
		b.Reset()
		_, err := remote.ReadPartialPacket(&b)
		if err != nil {
			errCh <- errors.Wrap(err, "read from remote failed")
			return
		}
		backend.SetResetOption(mysql.SeqResetOnWrite)
		err = backend.WritePacket(b.Bytes())
		if err == nil {
			err = backend.Flush()
		}
		if err != nil {
			errCh <- errors.Wrap(err, "write to backend failed")
			return
		}
	}
}

func copyOutboundPackets(remote, backend *mysql.Conn, errCh chan error) {
	var totalBytes int64
	var b bytes.Buffer
	for {
		b.Reset()
		n, err := backend.ReadPartialPacket(&b)
		if err != nil {
			errCh <- errors.Wrap(err, "read from backend failed")
			return
		}
		totalBytes += int64(n)
		remote.SetResetOption(mysql.SeqResetOnRead)
		err = remote.WritePacket(b.Bytes())
		if err != nil {
			errCh <- errors.Wrap(err, "write to remote failed")
			return
		}
		if b.Len() == 0 ||
			b.Bytes()[0] == mysql.HeaderOK ||
			b.Bytes()[0] == mysql.HeaderEOF ||
			b.Bytes()[0] == mysql.HeaderErr {
			err = remote.Flush()
			// if first byte is other value, it means it is paritial
			// result and there will be more packets so we don't
			// need to flush.
		}
		if err != nil {
			errCh <- errors.Wrap(err, "write to remote failed")
			return
		}
	}
}
