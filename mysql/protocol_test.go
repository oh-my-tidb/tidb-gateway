package mysql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtocol(t *testing.T) {
	hs1 := Handshake{
		ProtocolVersion: DefaultHandshakeVersion,
		ServerVersion:   "5.7.25-TiDB",
		ConnectionID:    1,
		AuthPluginData:  make([]byte, 20),
		Capability:      DefaultCapability,
		CharacterSet:    DefaultCollationID,
		StatusFlags:     ServerStatusAutocommit,
		AuthPluginName:  AuthNativePassword,
	}
	b := newBuffer(nil)
	hs1.Write(b)
	var hs2 Handshake
	b2 := newBuffer(b.Bytes())
	hs2.Read(b2)

	assert.Equal(t, toJson(hs2), toJson(hs1))
}

func toJson(x any) string {
	jb, _ := json.Marshal(x)
	return string(jb)
}
