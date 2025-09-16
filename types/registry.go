package types

import (
	"errors"
	"fmt"
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

type Type interface {
	Init() func() any

	// TODO: support schema?
	// Schema
}

type Registry interface {
	Codec() codec.Codec
	Init(t string) (any, error)
	Lookup(v any) (string, error)
	Marshal(v any) ([]byte, error)
	Unmarshal(b []byte, v any) error
	UnmarshalType(b []byte, t string) (any, error)
}

func validateTypeName(n string) error {
	if !nameRegex.MatchString(n) {
		return fmt.Errorf("%w: name %q has invalid characters", ErrTypeNotValid, n)
	}
	return nil
}
