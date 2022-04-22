package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/oh-my-tidb/tidb-gateway/gateway"
	"github.com/oh-my-tidb/tidb-gateway/utility"
)

var (
	addr           string
	backendConfigs gateway.BackendConfigs
)

func main() {
	flag.StringVar(&addr, "addr", ":3306", "listening address")
	flag.Var(&backendConfigs, "backend", "backend cluster configs")
	flag.Parse()

	log := utility.GetLogger()
	log.Infow("initializing gateway", "addr", addr, "backend", backendConfigs)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Errorw("failed to listen", "err", err)
		return
	}

	gw := gateway.New(lis, &backendConfigs)
	gw.StartServe()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Warnw("received signal", "signal", sig)
	gw.Stop()
}
