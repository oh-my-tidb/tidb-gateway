package mysql

type HandshakeResponse struct {
	Capability    uint32
	MaxPacketSize uint32
	CharacterSet  byte
	UserName      string
	DBName        string
	Auth          []byte
	AuthPlugin    string
	Attrs         map[string]string
}

func (s *HandshakeResponse) Write(b *Buffer) {
	// 4              capability flags
	b.WriteUint32(s.Capability)

	if s.Capability&ClientProtocol41 == 0 {
		// old format: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse320

		// 3              max-packet size
		b.WriteUint24(s.MaxPacketSize)
		// string[NUL]    username
		b.WriteStringNull(s.UserName)
		//   if capabilities & CLIENT_CONNECT_WITH_DB {
		// 	    string[NUL]    auth-response
		// 	    string[NUL]    database
		// 	} else {
		// 	    string[EOF]    auth-response
		// 	}
		b.WriteBytes(s.Auth)
		if s.Capability&ClientConnectWithDB != 0 {
			b.WriteByte(0x00)
			b.WriteBytes([]byte(s.DBName))
			b.WriteByte(0x00)
		}
		return
	}

	// new format: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41

	// 4              max-packet size
	b.WriteUint32(s.MaxPacketSize)
	// 1              character set
	b.WriteByte(s.CharacterSet)
	// string[23]     reserved (all [0])
	b.WriteBytes(make([]byte, 23))
	// string[NUL]    username
	b.WriteStringNull(s.UserName)
	//    if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
	// 	    lenenc-int     length of auth-response
	// 	    string[n]      auth-response
	// 	  } else if capabilities & CLIENT_SECURE_CONNECTION {
	// 	    1              length of auth-response
	// 	    string[n]      auth-response
	// 	  } else {
	// 	    string[NUL]    auth-response
	// 	  }
	if s.Capability&ClientPluginAuthLenencClientData != 0 {
		b.WriteLenencString(string(s.Auth))
	} else if s.Capability&ClientSecureConnection != 0 {
		b.WriteByte(byte(len(s.Auth)))
		b.WriteBytes(s.Auth)
	} else {
		b.WriteStringNull(string(s.Auth))
	}

	// if capabilities & CLIENT_CONNECT_WITH_DB {
	//   string[NUL]    database
	// }
	if s.Capability&ClientConnectWithDB != 0 {
		b.WriteStringNull(s.DBName)
	}

	// if capabilities & CLIENT_PLUGIN_AUTH {
	//   string[NUL]    auth plugin name
	// }
	if s.Capability&ClientPluginAuth != 0 {
		b.WriteStringNull(s.AuthPlugin)
	}

	//   if capabilities & CLIENT_CONNECT_ATTRS {
	//     lenenc-int     length of all key-values
	//     lenenc-str     key
	//     lenenc-str     value
	if s.Capability&ClientConnectAttrs != 0 {
		ab := newBuffer(nil)
		for k, v := range s.Attrs {
			ab.WriteLenencString(k)
			ab.WriteLenencString(v)
		}
		b.WriteLenencInt(uint64(ab.Len()))
		b.WriteBytes(ab.Bytes())
	}
}

func (s *HandshakeResponse) Read(b *Buffer) error {
	var err error
	// 4              capability flags
	s.Capability, err = b.ReadUint32()
	if s.Capability&ClientProtocol41 == 0 {
		// old format: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse320

		// 3              max-packet size
		s.MaxPacketSize, err = b.ReadUint24()
		if err != nil {
			return err
		}
		// string[NUL]    username
		s.UserName, err = b.ReadStringNull()
		if err != nil {
			return err
		}
		//   if capabilities & CLIENT_CONNECT_WITH_DB {
		// 	    string[NUL]    auth-response
		// 	    string[NUL]    database
		// 	} else {
		// 	    string[EOF]    auth-response
		// 	}
		if s.Capability&ClientConnectWithDB != 0 {
			auth, err := b.ReadStringNull()
			if err != nil {
				return err
			}
			s.Auth = []byte(auth)
			s.DBName, err = b.ReadStringNull()
			if err != nil {
				return err
			}
		} else {
			s.Auth = b.Bytes()
		}
		return nil
	}

	// new format: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41

	// 4              max-packet size
	s.MaxPacketSize, err = b.ReadUint32()
	if err != nil {
		return err
	}
	// 1              character set
	s.CharacterSet, err = b.ReadByte()
	if err != nil {
		return err
	}
	// string[23]     reserved (all [0])
	err = b.Skip(23)
	if err != nil {
		return err
	}
	// string[NUL]    username
	s.UserName, err = b.ReadStringNull()
	if err != nil {
		return err
	}
	//    if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
	// 	    lenenc-int     length of auth-response
	// 	    string[n]      auth-response
	// 	  } else if capabilities & CLIENT_SECURE_CONNECTION {
	// 	    1              length of auth-response
	// 	    string[n]      auth-response
	// 	  } else {
	// 	    string[NUL]    auth-response
	// 	  }
	if s.Capability&ClientPluginAuthLenencClientData != 0 {
		l, err := b.ReadLenencInt()
		if err != nil {
			return err
		}
		s.Auth, err = b.ReadBytes(int(l))
		if err != nil {
			return err
		}
	} else if s.Capability&ClientSecureConnection != 0 {
		l, err := b.ReadByte()
		if err != nil {
			return err
		}
		s.Auth, err = b.ReadBytes(int(l))
		if err != nil {
			return err
		}
	} else {
		auth, err := b.ReadStringNull()
		if err != nil {
			return err
		}
		s.Auth = []byte(auth)
	}
	// if capabilities & CLIENT_CONNECT_WITH_DB {
	//   string[NUL]    database
	// }
	if s.Capability&ClientConnectWithDB != 0 {
		s.DBName, err = b.ReadStringNull()
		if err != nil {
			return err
		}
	}
	// if capabilities & CLIENT_PLUGIN_AUTH {
	//   string[NUL]    auth plugin name
	// }
	if s.Capability&ClientPluginAuth != 0 {
		s.AuthPlugin, err = b.ReadStringNull()
		if err != nil {
			return err
		}
	}
	//   if capabilities & CLIENT_CONNECT_ATTRS {
	//     lenenc-int     length of all key-values
	//     lenenc-str     key
	//     lenenc-str     value
	if s.Capability&ClientConnectAttrs != 0 {
		l, err := b.ReadLenencInt()
		if err != nil {
			return err
		}
		data, err := b.ReadBytes(int(l))
		if err != nil {
			return err
		}
		ab := newBuffer(data)
		for ab.Len() > 0 {
			k, err := ab.ReadLenencString()
			if err != nil {
				return err
			}
			v, err := ab.ReadLenencString()
			if err != nil {
				return err
			}
			if s.Attrs == nil {
				s.Attrs = make(map[string]string)
			}
			s.Attrs[k] = v
		}
	}

	return nil
}
