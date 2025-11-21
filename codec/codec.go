package codec

import "errors"

var (
	ErrCodecNotRegistered = errors.New("rita: codec not registered")

	Default = JSON

	Codecs map[string]Codec
)

func init() {
	Codecs = make(map[string]Codec)
	for _, c := range []Codec{Binary, JSON, MsgPack, ProtoBuf} {
		Codecs[c.Name()] = c
	}
}

type Codec interface {
	Name() string
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}
