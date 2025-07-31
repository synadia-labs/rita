package codec

import (
	"testing"

	"github.com/synadia-labs/rita/testutil"
)

func TestBinaryCodec(t *testing.T) {
	is := testutil.NewIs(t)

	_, err := Binary.Marshal("foo")
	is.Err(err, nil)

	var s string
	err = Binary.Unmarshal([]byte("foo"), &s)
	is.Err(err, nil)

	b, err := Binary.Marshal([]byte("foo"))
	is.NoErr(err)
	is.Equal(b, []byte("foo"))

	// Ensure this resets the slice and performs a copy
	// to prevent unexpected byte changes from the source slice.
	x := []byte("barr")
	err = Binary.Unmarshal(b, &x)
	is.NoErr(err)
	is.Equal(x, []byte("foo"))

	b[0] = 'b'
	is.Equal(x, []byte("foo"))
}
