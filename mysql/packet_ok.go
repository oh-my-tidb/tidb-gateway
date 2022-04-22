package mysql

type OK struct {
	Header              byte
	AffectRows          int
	LastInsertID        int
	StatusFlags         uint16
	Warnings            uint16
	Info                string
	SessionStateChanges string
	Capability          uint32
}

func (p *OK) Write(b *Buffer) {
	b.WriteByte(p.Header)
	b.WriteLenencInt(uint64(p.AffectRows))
	b.WriteLenencInt(uint64(p.LastInsertID))
	if p.Capability&ClientProtocol41 != 0 {
		b.WriteUint16(p.StatusFlags)
		b.WriteUint16(p.Warnings)
	} else if p.Capability&ClientTransactions != 0 {
		b.WriteUint16(p.StatusFlags)
	}
	if p.Capability&ClientSessionTrack != 0 {
		b.WriteLenencString(p.Info)
		if p.Capability&uint32(ServerSessionStateChanged) != 0 {
			b.WriteLenencString(p.SessionStateChanges)
		}
	} else {
		b.WriteBytes([]byte(p.Info))
	}
}

func (p *OK) Read(b *Buffer) error {
	var err error
	p.Header, err = b.ReadByte()
	if err != nil {
		return err
	}
	return nil
}
