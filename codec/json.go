package codec

import "encoding/json"

var (
	JSON Codec = &jsonCodec{}
)

type jsonCodec struct{}

func (*jsonCodec) Name() string {
	return "json"
}

func (*jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)

}
func (*jsonCodec) Unmarshal(b []byte, v any) error {
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, v)
}
