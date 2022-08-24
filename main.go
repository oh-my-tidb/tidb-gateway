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
	addr                     string
	tlsCA                    string
	tlsCert                  string
	tlsKey                   string
	tlsVersion               string
	backendConfigs           gateway.BackendConfigs
	enableCompression        bool
	backendInsecureTransport bool
)

func main() {
	flag.StringVar(&addr, "addr", ":3306", "listening address")
	flag.StringVar(&tlsCA, "tls-ca", "", "TLS CA file")
	flag.StringVar(&tlsCert, "tls-cert", "", "TLS cert file")
	flag.StringVar(&tlsKey, "tls-key", "", "TLS key file")
	flag.StringVar(&tlsVersion, "tls-version", "", "Minimal TLS version (TLSv1.0/TLSv1.1/TLSv1.2/TLSv1.3)")
	flag.BoolVar(&enableCompression, "compress", false, "Enable compression")
	flag.Var(&backendConfigs, "backend", "backend cluster configs")
	flag.BoolVar(&backendInsecureTransport, "backend-insecure-transport", false, "Using insecure connection to backend")
	flag.Parse()

	log := utility.GetLogger()
	log.Infow("initializing gateway", "addr", addr, "backend", backendConfigs)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Errorw("failed to listen", "err", err)
		return
	}

	tlsConfig := gateway.TLSConfig{
		CA:         tlsCA,
		Cert:       tlsCert,
		Key:        tlsKey,
		MinVersion: tlsVersion,
	}

	gw, err := gateway.New(lis, &gateway.Config{
		TLS:                      tlsConfig,
		BackendConfigs:           backendConfigs,
		EnableCompression:        enableCompression,
		BackendInsecureTransport: backendInsecureTransport,
	})
	if err != nil {
		log.Errorw("failed to create gateway", "err", err)
		return
	}
	gw.StartServe()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Warnw("received signal", "signal", sig)
	gw.Stop()
}
