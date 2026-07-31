package rita

import (
	"fmt"

	"github.com/synadia-labs/rita/codec"
	"github.com/synadia-labs/rita/types"
)

// typeRegistry is the seam between the event store and type resolution /
// serialization. Exactly two implementations exist: registryTypes wraps a
// user-supplied *types.Registry, and binaryTypes is the default behind
// stores created without WithRegistry. Every operation touching event bytes
// goes through it, so "no registry" is a registry with degenerate semantics
// owned here — not a nil-check fork at each call site.
type typeRegistry interface {
	// resolveType returns the authoritative event type name given the
	// caller-declared name and the event data value.
	resolveType(declared string, v any) (string, error)

	// marshal serializes event data and reports the codec name to record in
	// the event's codec header.
	marshal(v any) (data []byte, codecName string, err error)

	// unmarshal materializes event data from stored bytes using the codec
	// named by the message's codec header.
	unmarshal(c codec.Codec, eventType string, data []byte) (any, error)
}

// registryTypes adapts a user-supplied *types.Registry: the registry owns
// the type-name ↔ Go-value mapping and the codec.
type registryTypes struct {
	r *types.Registry
}

func (t registryTypes) resolveType(declared string, v any) (string, error) {
	name, err := t.r.Lookup(v)
	if err != nil {
		return "", err
	}
	if declared != "" && declared != name {
		return "", fmt.Errorf("wrong type for event data: %s", declared)
	}
	return name, nil
}

func (t registryTypes) marshal(v any) ([]byte, string, error) {
	data, err := t.r.Marshal(v)
	if err != nil {
		return nil, "", err
	}
	return data, t.r.Codec().Name(), nil
}

func (t registryTypes) unmarshal(c codec.Codec, eventType string, data []byte) (any, error) {
	val, err := t.r.Init(eventType)
	if err != nil {
		return nil, err
	}
	if err := c.Unmarshal(data, val); err != nil {
		return nil, err
	}
	return val, nil
}

// binaryTypes is the degenerate registry for stores without a type registry:
// the caller owns type names (Type is required on append), bodies pass
// through codec.Binary untouched, and consumed events carry Data as a raw
// []byte. Its wire output is pinned by TestNoRegistryWireCompat.
type binaryTypes struct{}

func (binaryTypes) resolveType(declared string, _ any) (string, error) {
	if declared == "" {
		return "", ErrEventTypeRequired
	}
	return declared, nil
}

func (binaryTypes) marshal(v any) ([]byte, string, error) {
	data, err := codec.Binary.Marshal(v)
	if err != nil {
		return nil, "", err
	}
	return data, codec.Binary.Name(), nil
}

func (binaryTypes) unmarshal(c codec.Codec, _ string, data []byte) (any, error) {
	// There is no type to materialize; the payload is the value.
	var b []byte
	if err := c.Unmarshal(data, &b); err != nil {
		return nil, err
	}
	return b, nil
}
