package rabbit

type msgEvent struct {
	signal    func()
	typeEvent string
	reverceCh chan interface{}
	message   []byte
	offset    int64
}

func (m msgEvent) GetTypeMsg() string {
	return m.typeEvent
}

func (m msgEvent) GetReverceCh() chan interface{} {
	return m.reverceCh
}

func (m msgEvent) GetMsg() []byte {
	return m.message
}

func (m msgEvent) GetOffset() int64 {
	return m.offset
}
