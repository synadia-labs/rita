package types

import (
	"fmt"
	"reflect"

	"github.com/synadia-labs/rita/codec"
)

// InMemRegistry is used for transparently marshaling and unmarshaling messages
// and values from their native types to their network/storage representation.
type InMemRegistry struct {
	// Codec for marshaling and unmarshaling a values.
	codec codec.Codec

	// Index of types.
	types map[string]Type

	// Reflection type to the type name.
	rtypes map[reflect.Type]string
}

func (r *InMemRegistry) Codec() codec.Codec {
	return r.codec
}

type InMemType struct {
	InitFn func() any
}

func (t InMemType) Init() func() any {
	return t.InitFn
}

func (r *InMemRegistry) validate(name string, typ Type) error {
	if name == "" {
		return fmt.Errorf("%w: missing name", ErrTypeNotValid)
	}

	if err := validateTypeName(name); err != nil {
		return err
	}

	if typ.Init() == nil {
		return fmt.Errorf("%w: %s: missing init func", ErrTypeNotValid, name)
	}
	// Ensure the initialize value is not nil.
	v := typ.Init()()
	if v == nil {
		return fmt.Errorf("%w: %s: init func returns nil", ErrTypeNotValid, name)
	}

	// Get the Go type in order to transparently serialize to the correct name.
	rt := reflect.TypeOf(v)

	// Ensure the initialize type is a pointer so that deserialization works.
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("%w: %s: init func must return a pointer value", ErrTypeNotValid, name)
	}

	// Ensure that the pointer value is a struct type.
	if rt.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("%w: %s: value type must be a struct", ErrTypeNotValid, name)
	}

	// Ensure [de]serialization works in the base case.
	b, err := r.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("%w: %s: failed to marshal with codec: %s", ErrTypeNotValid, name, err)
	}

	err = r.codec.Unmarshal(b, v)
	if err != nil {
		return fmt.Errorf("%w: %s: failed to unmarshal with codec: %s", ErrTypeNotValid, name, err)
	}

	return nil
}

func (r *InMemRegistry) addType(name string, typ Type) {
	r.types[name] = typ

	// Initialize a value, reflect the type to index.
	v := typ.Init()()
	rt := reflect.TypeOf(v)

	r.rtypes[rt] = name
	r.rtypes[rt.Elem()] = name
}

// Init a value given the registered name of the type.
func (r *InMemRegistry) Init(t string) (any, error) {
	x, ok := r.types[t]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotRegistered, t)
	}

	v := x.Init()()
	return v, nil
}

// Lookup returns the registered name of the type given a value.
func (r *InMemRegistry) Lookup(v any) (string, error) {
	rt := reflect.TypeOf(v)
	t, ok := r.rtypes[rt]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrNoTypeForStruct, rt)
	}

	return t, nil
}

// Marshal serializes the value to a byte slice. This call
// validates the type is registered and delegates to the codec.
func (r *InMemRegistry) Marshal(v any) ([]byte, error) {
	_, err := r.Lookup(v)
	if err != nil {
		return nil, err
	}

	b, err := r.codec.Marshal(v)
	if err != nil {
		return b, fmt.Errorf("%T: marshal error: %w", v, err)
	}
	return b, nil
}

// Unmarshal deserializes a byte slice into the value. This call
// validates the type is registered and delegates to the codec.
func (r *InMemRegistry) Unmarshal(b []byte, v any) error {
	_, err := r.Lookup(v)
	if err != nil {
		return err
	}

	err = r.codec.Unmarshal(b, v)
	if err != nil {
		return fmt.Errorf("%T: unmarshal error: %w", v, err)
	}
	return nil
}

// UnmarshalType initializes a new value for the registered type,
// unmarshals the byte slice, and returns it.
func (r *InMemRegistry) UnmarshalType(b []byte, t string) (any, error) {
	v, err := r.Init(t)
	if err != nil {
		return nil, err
	}
	err = r.Unmarshal(b, v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func NewInMemRegistry(types map[string]Type, c codec.Codec) (Registry, error) {
	r := &InMemRegistry{
		codec:  c,
		types:  make(map[string]Type),
		rtypes: make(map[reflect.Type]string),
	}

	for n, t := range types {
		err := r.validate(n, t)
		if err != nil {
			return nil, err
		}
		r.addType(n, t)
	}

	return r, nil
}
