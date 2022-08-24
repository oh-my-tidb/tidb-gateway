package mysql

// Err represnets a MySQL packet that contains an error.
type Err struct {
	Header     byte
	Code       uint16
	State      string
	Message    string
	Capability uint32
}

// Write writes the packet to a buffer.
func (e *Err) Write(b *Buffer) {
	b.WriteByte(e.Header)
	b.WriteUint16(e.Code)
	if e.Capability&ClientProtocol41 != 0 {
		b.WriteByte('#')
		b.WriteBytes([]byte(e.State))
	}
	b.WriteBytes([]byte(e.Message))
}

// Read reads packet from a buffer.
func (e *Err) Read(b *Buffer) error {
	panic("implemented")
}
