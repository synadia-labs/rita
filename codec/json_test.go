package codec

import (
	"testing"
	"time"
)

func BenchmarkJSONMarshal(b *testing.B) {
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
		_, _ = JSON.Marshal(v1)
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
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

	y, _ := JSON.Marshal(v1)
	var v2 T

	b.ResetTimer()

	for b.Loop() {
		_ = JSON.Unmarshal(y, &v2)
	}
}
