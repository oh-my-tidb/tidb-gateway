package mysql

import "github.com/pkg/errors"

// Handshake is the initial handshake packet sent from server to client.
type Handshake struct {
	ProtocolVersion uint8
	ServerVersion   string
	ConnectionID    uint32
	AuthPluginData  []byte
	Capability      uint32
	CharacterSet    uint8
	StatusFlags     uint16
	AuthPluginName  string
}

// Write writes the packet to a buffer.
func (s *Handshake) Write(b *Buffer) {
	// 1              [0a] protocol version
	b.WriteByte(s.ProtocolVersion)
	// string[NUL]    server version
	b.WriteStringNull(s.ServerVersion)
	// 4              connection id
	b.WriteUint32(s.ConnectionID)
	// string[8]      auth-plugin-data-part-1
	b.WriteBytes(s.AuthPluginData[:8])
	// 1              [00] filler
	b.WriteByte(0x00)
	// 2              capability flags (lower 2 bytes)
	b.WriteUint16(uint16(s.Capability & 0xFFFF))
	// 1              character set
	b.WriteByte(s.CharacterSet)
	// 2              status flags
	b.WriteUint16(s.StatusFlags)
	// 2              capability flags (upper 2 bytes)
	b.WriteUint16(uint16(s.Capability >> 16))
	//  if capabilities & CLIENT_PLUGIN_AUTH {
	// 	    1              length of auth-plugin-data
	// 	} else {
	// 	    1              [00]
	// 	}
	if s.Capability&ClientPluginAuth != 0 {
		b.WriteByte(byte(len(s.AuthPluginData) + 1))
	} else {
		b.WriteByte(0x00)
	}
	// string[10]     reserved (all [00])
	b.WriteBytes(make([]byte, 10))
	//   if capabilities & CLIENT_SECURE_CONNECTION {
	//     string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
	if s.Capability&ClientSecureConnection != 0 {
		l := len(s.AuthPluginData) - 8
		b.WriteBytes(s.AuthPluginData[8 : 8+l])
		if l < 13 {
			b.WriteBytes(make([]byte, 13-l))
		}
	}
	//   if capabilities & CLIENT_PLUGIN_AUTH {
	//    string[NUL]    auth-plugin name
	if s.Capability&ClientPluginAuth != 0 {
		b.WriteStringNull(s.AuthPluginName)
	}
}

// Read reads the packet from a buffer.
// Support V9 or V10.
func (s *Handshake) Read(b *Buffer) error {
	var err error
	// 1              [0a] or [09] protocol version
	s.ProtocolVersion, err = b.ReadByte()
	if err != nil {
		return err
	}
	if s.ProtocolVersion != 10 && s.ProtocolVersion != 9 {
		return errors.New("only support protocol v9 or v10")
	}

	// string[NUL]    server version
	s.ServerVersion, err = b.ReadStringNull()
	if err != nil {
		return err
	}

	// 4              connection id
	s.ConnectionID, err = b.ReadUint32()
	if err != nil {
		return err
	}

	if s.ProtocolVersion == 9 {
		// string[NUL]    scramble
		str, err := b.ReadStringNull()
		if err != nil {
			return err
		}
		s.AuthPluginData = []byte(str)
		return nil
	}

	// string[8]      auth-plugin-data-part-1
	data, err := b.ReadBytes(8)
	if err != nil {
		return err
	}
	s.AuthPluginData = append(s.AuthPluginData, data...)

	// 1              [00] filler
	if _, err := b.ReadByte(); err != nil {
		return err
	}

	// 2              capability flags (lower 2 bytes)
	capLow, err := b.ReadUint16()
	if err != nil {
		return err
	}
	s.Capability = uint32(capLow)

	if b.Len() == 0 {
		return nil
	}
	//   if more data in the packet:

	// 1              character set
	s.CharacterSet, err = b.ReadByte()
	if err != nil {
		return err
	}

	// 2              status flags
	s.StatusFlags, err = b.ReadUint16()
	if err != nil {
		return err
	}

	// 2              capability flags (upper 2 bytes)
	capHigh, err := b.ReadUint16()
	if err != nil {
		return err
	}
	s.Capability |= uint32(capHigh) << 16

	//  if capabilities & CLIENT_PLUGIN_AUTH {
	// 	    1              length of auth-plugin-data
	// 	} else {
	// 	    1              [00]
	// 	}
	var authDataLen byte
	authDataLen, err = b.ReadByte()
	if err != nil {
		return err
	}

	// string[10]     reserved (all [00])
	if err = b.Skip(10); err != nil {
		return err
	}

	//   if capabilities & CLIENT_SECURE_CONNECTION {
	//     string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
	if s.Capability&ClientSecureConnection != 0 {
		l := int(authDataLen) - 8 - 1
		data, err = b.ReadBytes(l)
		if err != nil {
			return err
		}
		if l < 13 {
			err = b.Skip(13 - l)
			if err != nil {
				return err
			}
		}
		s.AuthPluginData = append(s.AuthPluginData, data...)
	}

	//   if capabilities & CLIENT_PLUGIN_AUTH {
	//    string[NUL]    auth-plugin name
	if s.Capability&ClientPluginAuth != 0 {
		s.AuthPluginName, err = b.ReadStringNull()
		if err != nil {
			return err
		}
	}

	return nil
}
