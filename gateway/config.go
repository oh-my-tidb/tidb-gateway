package gateway

import (
	"errors"
	"strings"
)

type BackendConfig struct {
	ClusterID string
	Address   string
}

type BackendConfigs []BackendConfig

func (b *BackendConfigs) String() string {
	return "backend clusters"
}

func (b *BackendConfigs) Set(value string) error {
	splits := strings.SplitN(value, "=", 2)
	if len(splits) != 2 {
		return errors.New("backend must be in the form of clusterID=address")
	}
	*b = append(*b, BackendConfig{ClusterID: splits[0], Address: splits[1]})
	return nil
}

func (b *BackendConfigs) Find(cluster string) string {
	for _, c := range *b {
		if strings.EqualFold(c.ClusterID, cluster) {
			return c.Address
		}
	}
	return cluster
}

// TLSConfig is used to establish TLS connection.
type TLSConfig struct {
	CA         string
	Cert       string
	Key        string
	MinVersion string
}

// Config is used to configure a gateway.
type Config struct {
	TLS                      TLSConfig
	BackendConfigs           BackendConfigs
	EnableCompression        bool
	BackendInsecureTransport bool
}
