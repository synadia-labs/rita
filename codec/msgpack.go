package codec

import (
	"github.com/vmihailenco/msgpack/v5"
)

var (
	MsgPack Codec = &msgpackCodec{}
)

type msgpackCodec struct{}

func (*msgpackCodec) Name() string {
	return "msgpack"
}

func (*msgpackCodec) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (*msgpackCodec) Unmarshal(b []byte, v any) error {
	return msgpack.Unmarshal(b, v)
}
