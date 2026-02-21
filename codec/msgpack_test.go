package codec

import (
	"testing"
	"time"

	"github.com/synadia-labs/rita/testutil"
)

func TestMsgPackCodec(t *testing.T) {
	is := testutil.NewIs(t)

	type T struct {
		Name  string
		Count int
	}

	v := &T{Name: "bar", Count: 7}

	b, err := MsgPack.Marshal(v)
	is.NoErr(err)

	var out T
	err = MsgPack.Unmarshal(b, &out)
	is.NoErr(err)
	is.Equal(out, *v)
}

func TestMsgPackCodec_Name(t *testing.T) {
	is := testutil.NewIs(t)
	is.Equal(MsgPack.Name(), "msgpack")
}

func BenchmarkMsgPackMarshal(b *testing.B) {
	type T struct {
		String string
		Int    int
		Bool   bool
		Float  float32
		Struct *T
		Time   time.Time
		Bytes  []byte
	}

	v1 := &T{
		String: "foo",
		Int:    5,
		Bool:   true,
		Float:  1.4,
		Struct: &T{
			Int: 10,
		},
		Time:  time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
		Bytes: []byte(`{"foo": "bar", "baz": 3.4}`),
	}

	b.ResetTimer()

	for b.Loop() {
		_, _ = MsgPack.Marshal(v1)
	}
}

func BenchmarkMsgPackUnmarshal(b *testing.B) {
	type T struct {
		String string
		Int    int
		Bool   bool
		Float  float32
		Struct *T
		Time   time.Time
		Bytes  []byte
	}

	v1 := &T{
		String: "foo",
		Int:    5,
		Bool:   true,
		Float:  1.4,
		Struct: &T{
			Int: 10,
		},
		Time:  time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
		Bytes: []byte(`{"foo": "bar", "baz": 3.4}`),
	}

	y, _ := MsgPack.Marshal(v1)
	var v2 T

	b.ResetTimer()

	for b.Loop() {
		_ = MsgPack.Unmarshal(y, &v2)
	}
}
