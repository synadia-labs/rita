package types

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/synadia-labs/rita/codec"
	schemaregistry "github.com/synadia-labs/schema-registry"
)

const (
	defaultSchemaBucket = "schemas"
	uriBase             = "file://io.nexus"
)

var (
	_ Type     = (*NatsType)(nil)
	_ Registry = (*NatsRegistry)(nil)
)

// NatsRegistry is used for transparently marshaling and unmarshaling messages
// and values from their native types to their network/storage representation.
type NatsRegistry struct {
	ctx    context.Context
	logger *slog.Logger
	// Codec for marshaling and unmarshaling a values.
	codec codec.Codec

	// Schema registry for storing and retrieving schemas
	registry schemaregistry.Registry

	// JetStream for KV operations
	js jetstream.JetStream

	// In-memory stores for type information
	types   map[string]Type               // Maps schema name -> Type interface
	rtypes  map[string]string             // Reverse: maps Go type string -> schema name
	schemas map[string]*jsonschema.Schema // Compiled schemas for validation
}

type NatsType struct {
	InitFn      func() any
	Name        string
	Description string
	DocPath     string
}

func (t NatsType) Init() func() any {
	return t.InitFn
}

func (r *NatsRegistry) Codec() codec.Codec {
	return r.codec
}

// Init a value given the registered name of the type.
func (r *NatsRegistry) Init(t string) (any, error) {
	typ, ok := r.types[t]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotRegistered, t)
	}

	initFn := typ.Init()
	if initFn == nil {
		return nil, fmt.Errorf("%w: %s has nil init function", ErrTypeNotValid, t)
	}

	v := initFn()
	return v, nil
}

// Lookup returns the registered name of the type given a value.
func (r *NatsRegistry) Lookup(v any) (string, error) {
	rt := reflect.TypeOf(v)
	fullTypeName := rt.String()

	schemaName, ok := r.rtypes[fullTypeName]
	if ok {
		return schemaName, nil
	}

	return "", fmt.Errorf("%w: %s", ErrNoTypeForStruct, rt)
}

// Marshal serializes the value to a byte slice. This call
// validates the type is registered and delegates to the codec.
func (r *NatsRegistry) Marshal(v any) ([]byte, error) {
	schemaName, err := r.Lookup(v)
	if err != nil {
		return nil, err
	}

	b, err := r.codec.Marshal(v)
	if err != nil {
		return b, fmt.Errorf("%T: marshal error: %w", v, err)
	}

	// Validate against schema if we have one
	if schema, ok := r.schemas[schemaName]; ok {
		// For validation, we need JSON representation
		jsonData, err := r.codec.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal for validation: %w", err)
		}

		switch r.codec {
		case codec.JSON:
			inst, err := jsonschema.UnmarshalJSON(strings.NewReader(string(jsonData)))
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal for validation: %w", err)
			}

			if err := schema.Validate(inst); err != nil {
				return nil, fmt.Errorf("validation failed: %w", err)
			}
		default:
			if r.logger != nil {
				r.logger.Warn("validation skipped: provided codec not supported", slog.String("codec", r.codec.Name()))
			}
		}
	}

	return b, nil
}

// Unmarshal deserializes a byte slice into the value. This call
// validates the type is registered and delegates to the codec.
func (r *NatsRegistry) Unmarshal(b []byte, v any) error {
	schemaName, err := r.Lookup(v)
	if err != nil {
		return err
	}

	err = r.codec.Unmarshal(b, v)
	if err != nil {
		return fmt.Errorf("%T: unmarshal error: %w", v, err)
	}

	if schema, ok := r.schemas[schemaName]; ok {
		data, err := r.codec.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal for validation: %w", err)
		}

		switch r.codec {
		case codec.JSON:
			inst, err := jsonschema.UnmarshalJSON(strings.NewReader(string(data)))
			if err != nil {
				return fmt.Errorf("failed to unmarshal for validation: %w", err)
			}

			if err := schema.Validate(inst); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}
		default:
			if r.logger != nil {
				r.logger.Warn("validation skipped: provided codec not supported", slog.String("codec", r.codec.Name()))
			}
		}
	}

	return nil
}

// UnmarshalType initializes a new value for the registered type,
// unmarshals the byte slice, and returns it.
func (r *NatsRegistry) UnmarshalType(b []byte, t string) (any, error) {
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

func NewNatsRegistry(ctx context.Context, logger *slog.Logger, types map[string]Type, c codec.Codec, nc *nats.Conn) (Registry, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("creating jetstream context: %w", err)
	}

	// Create or get the schema registry KV store
	schemaKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: defaultSchemaBucket,
	})
	if err != nil && err != jetstream.ErrBucketExists {
		return nil, fmt.Errorf("creating schema key-value store: %w", err)
	}

	if errors.Is(err, jetstream.ErrBucketExists) {
		schemaKV, err = js.KeyValue(ctx, defaultSchemaBucket)
		if err != nil {
			return nil, fmt.Errorf("getting schema key-value store: %w", err)
		}
	}

	r := &NatsRegistry{
		ctx:      ctx,
		logger:   logger,
		codec:    c,
		registry: schemaregistry.NewRegistry(ctx, js, schemaKV),
		js:       js,
		types:    make(map[string]Type),
		rtypes:   make(map[string]string),
		schemas:  make(map[string]*jsonschema.Schema),
	}

	// Process each type registration
	for schemaName, t := range types {
		// Validate basic requirements
		if schemaName == "" {
			return nil, fmt.Errorf("%w: missing schema name", ErrTypeNotValid)
		}

		if err := validateTypeName(schemaName); err != nil {
			return nil, err
		}

		// Store the type in memory
		r.types[schemaName] = t

		// Get the init function from the type
		initFn := t.Init()

		natsType, ok := t.(NatsType)
		if !ok {
			// For non-NatsType (e.g., InMemType), we still need to register it
			if initFn == nil {
				return nil, fmt.Errorf("%w: %s: init func is nil", ErrTypeNotValid, schemaName)
			}

			v := initFn()
			if v == nil {
				return nil, fmt.Errorf("%w: %s: init func returns nil", ErrTypeNotValid, schemaName)
			}

			// Store type mapping in memory
			r.storeTypeMapping(schemaName, v)

			continue
		}

		// Process NatsType with schema registration
		if natsType.DocPath != "" {
			// Read schema definition from file
			schemaDefinition, err := os.ReadFile(natsType.DocPath)
			if err != nil {
				return nil, fmt.Errorf("reading schema file %s: %w", natsType.DocPath, err)
			}

			// Add schema to registry
			_, err = r.registry.Add(ctx, schemaName, &schemaregistry.AddRequest{
				Name:         schemaName,
				Definition:   string(schemaDefinition),
				Format:       schemaregistry.SchemaTypeJSONSchema,
				CompatPolicy: schemaregistry.SchemaCompatPolicyNone,
				Description:  natsType.Description,
				Metadata:     map[string]string{},
			})
			if err != nil {
				// Check if schema already exists - if so, that's okay
				if !errors.Is(err, jetstream.ErrKeyExists) &&
					!strings.Contains(err.Error(), "key exists") &&
					!strings.Contains(err.Error(), "already exists") {
					return nil, fmt.Errorf("failed to add schema %s: %w", schemaName, err)
				}
				if logger != nil {
					logger.Debug("schema already exists", "schema", schemaName)
				}
			}
		}

		// Compile and store the schema if provided
		if natsType.DocPath != "" {
			schemaData, err := os.ReadFile(natsType.DocPath)
			if err == nil {
				c := jsonschema.NewCompiler()
				doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(schemaData)))
				if err != nil {
					return nil, fmt.Errorf("failed to parse schema JSON %s: %w", natsType.DocPath, err)
				}
				if err := c.AddResource(schemaName, doc); err != nil {
					return nil, fmt.Errorf("failed to add schema resource %s: %w", schemaName, err)
				}
				schema, err := c.Compile(schemaName)
				if err != nil {
					return nil, fmt.Errorf("failed to compile schema %s: %w", schemaName, err)
				}
				r.schemas[schemaName] = schema
				if logger != nil {
					logger.Debug("compiled schema", "schema", schemaName, "path", natsType.DocPath)
				}
			}
		}

		// Store type mapping in memory using the already stored InitFn
		if initFn != nil {
			v := initFn()
			if v != nil {
				r.storeTypeMapping(schemaName, v)
			}
		}
	}

	return r, nil
}

// storeTypeMapping stores the Go type metadata in memory
func (r *NatsRegistry) storeTypeMapping(schemaName string, v any) {
	rt := reflect.TypeOf(v)
	fullTypeName := rt.String()

	// Store reverse mapping
	r.rtypes[fullTypeName] = schemaName

	// Handle pointer/non-pointer types
	if rt.Kind() != reflect.Ptr {
		// Also store the pointer version
		pointerType := "*" + fullTypeName
		r.rtypes[pointerType] = schemaName
	} else {
		// Also store the non-pointer version
		elemType := rt.Elem()
		r.rtypes[elemType.String()] = schemaName
	}
}
