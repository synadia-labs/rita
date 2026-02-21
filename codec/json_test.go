package codec

import (
	"testing"
	"time"

	"github.com/synadia-labs/rita/testutil"
)

func TestJSONCodec(t *testing.T) {
	is := testutil.NewIs(t)

	type T struct {
		Name  string
		Count int
	}

	v := &T{Name: "foo", Count: 42}

	b, err := JSON.Marshal(v)
	is.NoErr(err)

	var out T
	err = JSON.Unmarshal(b, &out)
	is.NoErr(err)
	is.Equal(out, *v)
}

func TestJSONCodec_Name(t *testing.T) {
	is := testutil.NewIs(t)
	is.Equal(JSON.Name(), "json")
}

func TestJSONCodec_EmptyInput(t *testing.T) {
	var out struct{}
	err := JSON.Unmarshal([]byte{}, &out)
	if err != nil {
		t.Fatalf("expected nil error for empty input, got: %v", err)
	}
}

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
