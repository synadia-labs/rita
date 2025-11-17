package codec

import "errors"

var (
	ErrCodecNotRegistered = errors.New("rita: codec not registered")

	Default = JSON

	Codecs = map[string]Codec{
		Binary.Name():   Binary,
		JSON.Name():     JSON,
		MsgPack.Name():  MsgPack,
		ProtoBuf.Name(): ProtoBuf,
	}
)

type Codec interface {
	Name() string
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}
