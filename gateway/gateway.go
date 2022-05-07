package gateway

import (
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/oh-my-tidb/tidb-gateway/mysql"
	"github.com/oh-my-tidb/tidb-gateway/utility"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Gateway struct {
	log          *zap.SugaredLogger
	l            net.Listener
	conf         *BackendConfigs
	quit         chan struct{}
	wg           sync.WaitGroup
	connectionID uint32
}

func New(l net.Listener, conf *BackendConfigs) *Gateway {
	return &Gateway{
		log:  utility.GetLogger(),
		conf: conf,
		l:    l,
		quit: make(chan struct{}),
	}
}

func (g *Gateway) Stop() {
	g.log.Info("gateway starts to stop")
	close(g.quit)
	g.l.Close()
	g.wg.Wait()
	g.log.Sync()
}

func (g *Gateway) StartServe() {
	g.wg.Add(1)
	go g.serve()
}

func (g *Gateway) serve() {
	defer g.wg.Done()
	g.log.Info("gateway starts to accept connections")
	for {
		conn, err := g.l.Accept()
		if err != nil {
			return
		}
		g.wg.Add(1)
		go g.handleConn(conn)
	}
}

func (g *Gateway) handleConn(rawConn net.Conn) {
	defer g.wg.Done()

	connID := atomic.AddUint32(&g.connectionID, 1)
	// TODO: set keepalive and nodelay options
	g.log.Infow("accepting new connection", "connID", connID)
	conn := mysql.NewConn(rawConn)
	defer conn.Close()

	if err := g.sendInitialHandshake(conn, connID); err != nil {
		g.log.Warnw("failed to send initial handshake", "connID", connID, "err", err)
		return
	}

	res, err := g.recvHandshakeResponse(conn)
	if err != nil {
		g.log.Warnw("failed to recv handshake response", "connID", connID, "err", err)
		return
	}

	backendAddr, err := g.getBackendAddr(res)
	if err != nil {
		g.log.Warnw("failed to get cluster address", "connID", connID, "err", err)
		g.sendErr(conn, err.Error())
		return
	}

	g.log.Infow("start to connect backend", "connID", connID, "backend", backendAddr)

	backendConn, err := g.connectBackend(backendAddr)
	if err != nil {
		g.log.Errorw("failed to connect backend", "connID", connID, "err", err)
		g.sendErr(conn, err.Error())
		return
	}
	defer backendConn.Close()

	_, err = g.recvInitialHandshake(backendConn)
	if err != nil {
		g.log.Errorw("recv initial handshake from backend failed", "connID", connID, "err", err)
		g.sendErr(conn, err.Error())
		return
	}

	// We do not really care about the content of InitialHandshake here.
	// Simply redirect remote's response to backend.
	// Hopefully they can come to a consensus.

	if err := backendConn.SendPacket(res); err != nil {
		g.log.Errorw("failed to send handshake response to backend", "connID", connID, "err", err)
		g.sendErr(conn, err.Error())
		return
	}

	g.log.Infow("start to relay data", "connID", connID, "backend", backendAddr)

	quit := make(chan struct{}, 1)
	go func() {
		_, err := io.Copy(conn.RawConn(), backendConn.RawConn())
		g.log.Warnw("remote -> backend closed", "connID", connID, "err", err)
		quit <- struct{}{}
	}()
	go func() {
		_, err := io.Copy(backendConn.RawConn(), conn.RawConn())
		g.log.Warnw("backend -> remote closed", "connID", connID, "err", err)
		quit <- struct{}{}
	}()
	select {
	case <-quit:
	case <-g.quit:
	}
	g.log.Infow("connection is closed", "connID", connID)
}

func (g *Gateway) sendInitialHandshake(conn *mysql.Conn, connID uint32) error {
	hs := &mysql.Handshake{
		ProtocolVersion: mysql.DefaultHandshakeVersion,
		ServerVersion:   "5.7.25-TiDB",
		ConnectionID:    connID,
		AuthPluginData:  make([]byte, 20),
		Capability:      mysql.DefaultCapability,
		CharacterSet:    mysql.DefaultCollationID,
		StatusFlags:     mysql.ServerStatusAutocommit,
		AuthPluginName:  mysql.AuthNativePassword,
	}
	return conn.SendPacket(hs)
}

func (g *Gateway) recvInitialHandshake(conn *mysql.Conn) (*mysql.Handshake, error) {
	var hs mysql.Handshake
	if err := conn.RecvPacket(&hs); err != nil {
		return nil, err
	}
	return &hs, nil
}

func (g *Gateway) recvHandshakeResponse(conn *mysql.Conn) (*mysql.HandshakeResponse, error) {
	var res mysql.HandshakeResponse
	if err := conn.RecvPacket(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (g *Gateway) sendErr(conn *mysql.Conn, msg string) {
	err := &mysql.Err{
		Header:     mysql.HeaderErr,
		Code:       mysql.ErrCodeUnknown,
		State:      mysql.UnknownState,
		Message:    msg,
		Capability: mysql.DefaultCapability,
	}
	conn.SendPacket(err)
}

func (g *Gateway) getBackendAddr(res *mysql.HandshakeResponse) (string, error) {
	if res.DBName == "" {
		return "", errors.New("no db name")
	}

	var clusterID string
	if splits := strings.SplitN(res.DBName, "/", 2); len(splits) == 1 {
		clusterID, res.DBName = splits[0], ""
	} else {
		clusterID, res.DBName = splits[0], splits[1]
	}

	clusterAddr := g.conf.Find(clusterID)
	if ok, _ := regexp.MatchString(`:\d+$`, clusterAddr); !ok {
		clusterAddr = clusterAddr + ":4000"
	}

	return clusterAddr, nil
}

func (g *Gateway) connectBackend(addr string) (*mysql.Conn, error) {
	rawConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return mysql.NewConn(rawConn), nil
}
