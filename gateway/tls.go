package gateway

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/pkg/errors"
)

func loadTLSConfig(ca, cert, key, version string) (*tls.Config, error) {
	if ca == "" && cert == "" && key == "" {
		return nil, nil
	}

	var tlsConfig tls.Config
	if ca != "" {
		caCert, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read ca")
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}
	if cert != "" && key != "" {
		cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load key pair")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	tlsConfig.MinVersion = tls.VersionTLS12
	switch version {
	case "TLSv1.0":
		tlsConfig.MinVersion = tls.VersionTLS10
	case "TLSv1.1":
		tlsConfig.MinVersion = tls.VersionTLS11
	case "TLSv1.2":
		tlsConfig.MinVersion = tls.VersionTLS12
	case "TLSv1.3":
		tlsConfig.MinVersion = tls.VersionTLS13
	}
	return &tlsConfig, nil
}
