package mysql

type Err struct {
	Header     byte
	Code       uint16
	State      string
	Message    string
	Capability uint32
}

func (e *Err) Write(b *Buffer) {
	b.WriteByte(e.Header)
	b.WriteUint16(e.Code)
	if e.Capability&ClientProtocol41 != 0 {
		b.WriteByte('#')
		b.WriteBytes([]byte(e.State))
	}
	b.WriteBytes([]byte(e.Message))
}

func (e *Err) Read(b *Buffer) error {
	panic("implemented")
}
