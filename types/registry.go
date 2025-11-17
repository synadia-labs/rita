package types

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"

	"github.com/synadia-labs/rita/codec"
)

var (
	ErrTypeNotValid      = errors.New("rita: type not valid")
	ErrTypeNotRegistered = errors.New("rita: type not registered")
	ErrNoTypeForStruct   = errors.New("rita: no type for struct")
	ErrMarshal           = errors.New("rita: marshal error")
	ErrUnmarshal         = errors.New("rita: unmarshal error")

	nameRegex = regexp.MustCompile(`^[\w-]+(\.[\w-]+)*$`)
)

// Validator can be optionally implemented on user-defined types.
type Validator interface {
	Validate() error
}

func validateTypeName(n string) error {
	if !nameRegex.MatchString(n) {
		return fmt.Errorf("%w: name %q has invalid characters", ErrTypeNotValid, n)
	}
	return nil
}

type Type struct {
	Init func() any

	// TODO: support schema?
	// Schema
}

type registryOption func(o *Registry) error

func (f registryOption) addOption(o *Registry) error {
	return f(o)
}

// RegistryOption models a option when creating a type registry.
type RegistryOption interface {
	addOption(o *Registry) error
}

// Codec is a registry option to define the desired serialization codec.
func Codec(name string) RegistryOption {
	return registryOption(func(o *Registry) error {
		c, ok := codec.Codecs[name]
		if !ok {
			return fmt.Errorf("%w: %s", codec.ErrCodecNotRegistered, name)
		}

		o.codec = c
		return nil
	})
}

// Registry is used for transparently marshaling and unmarshaling messages
// and values from their native types to their network/storage representation.
type Registry struct {
	// Codec for marshaling and unmarshaling a values.
	codec codec.Codec

	// Index of types.
	types map[string]*Type

	// Reflection type to the type name.
	rtypes map[reflect.Type]string
}

func (r *Registry) Codec() codec.Codec {
	return r.codec
}

func (r *Registry) validate(name string, typ *Type) error {
	if name == "" {
		return fmt.Errorf("%w: missing name", ErrTypeNotValid)
	}

	if err := validateTypeName(name); err != nil {
		return err
	}

	if typ.Init == nil {
		return fmt.Errorf("%w: %s: init func is nil", ErrTypeNotValid, name)
	}

	// Ensure the initialize value is not nil.
	v := typ.Init()
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

func (r *Registry) addType(name string, typ *Type) {
	r.types[name] = typ

	// Initialize a value, reflect the type to index.
	v := typ.Init()
	rt := reflect.TypeOf(v)

	r.rtypes[rt] = name
	r.rtypes[rt.Elem()] = name
}

// Init a value given the registered name of the type.
func (r *Registry) Init(t string) (any, error) {
	x, ok := r.types[t]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotRegistered, t)
	}

	v := x.Init()
	return v, nil
}

// Lookup returns the registered name of the type given a value.
func (r *Registry) Lookup(v any) (string, error) {
	rt := reflect.TypeOf(v)
	t, ok := r.rtypes[rt]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrNoTypeForStruct, rt)
	}

	return t, nil
}

// Marshal serializes the value to a byte slice. This call
// validates the type is registered and delegates to the codec.
func (r *Registry) Marshal(v any) ([]byte, error) {
	if vv, ok := v.(Validator); ok {
		if err := vv.Validate(); err != nil {
			return nil, err
		}
	}

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
func (r *Registry) Unmarshal(b []byte, v any) error {
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
func (r *Registry) UnmarshalType(b []byte, t string) (any, error) {
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

func NewRegistry(types map[string]*Type, opts ...RegistryOption) (*Registry, error) {
	r := &Registry{
		codec:  codec.Default,
		types:  make(map[string]*Type),
		rtypes: make(map[reflect.Type]string),
	}

	for _, f := range opts {
		if err := f.addOption(r); err != nil {
			return nil, err
		}
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
