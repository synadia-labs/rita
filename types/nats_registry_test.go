package types

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-labs/rita/codec"
	"github.com/synadia-labs/rita/testutil"
)

// cleanupSchemasBucket removes the schemas bucket for test cleanup
func cleanupSchemasBucket(t *testing.T, nc *nats.Conn) {
	js, err := jetstream.New(nc)
	if err != nil {
		return
	}

	// Try to delete the schema bucket - ignore errors if it doesn't exist
	_ = js.DeleteKeyValue(context.Background(), "schemas")
}

// Test types that work with JSON Schema
type Person struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Email string `json:"email,omitempty"`
}

type Order struct {
	ID         string  `json:"id"`
	CustomerID string  `json:"customer_id"`
	Total      float64 `json:"total"`
	Items      []Item  `json:"items"`
}

type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

func TestNatsTypeInit(t *testing.T) {
	is := testutil.NewIs(t)

	// Test that NatsType.Init() returns the InitFn
	nt := NatsType{
		InitFn: func() any { return &Person{} },
		Name:   "Person",
	}

	initFn := nt.Init()
	is.True(initFn != nil)

	// Test that calling the function creates new instances
	v1 := initFn()
	v2 := initFn()
	is.True(v1 != v2) // Different instances
	is.True(v1 != nil)
	is.True(v2 != nil)
}

func TestNewNatsRegistry(t *testing.T) {
	is := testutil.NewIs(t)

	// Create test schema files
	tmpDir := t.TempDir()

	// Person schema
	personSchemaPath := filepath.Join(tmpDir, "person.json")
	personSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"name": {
				"type": "string",
				"minLength": 1
			},
			"age": {
				"type": "integer",
				"minimum": 0,
				"maximum": 150
			},
			"email": {
				"type": "string",
				"format": "email"
			}
		},
		"required": ["name", "age"]
	}`
	err := os.WriteFile(personSchemaPath, []byte(personSchema), 0644)
	is.NoErr(err)

	// Invalid schema
	invalidSchemaPath := filepath.Join(tmpDir, "invalid.json")
	err = os.WriteFile(invalidSchemaPath, []byte(`{invalid json`), 0644)
	is.NoErr(err)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	// Clean up schemas bucket after test
	t.Cleanup(func() {
		cleanupSchemasBucket(t, nc)
	})

	// Not serializable type
	type NotSerializable struct {
		C chan int
	}

	tests := map[string]struct {
		Types map[string]Type
		Err   bool
	}{
		"valid-nats-type": {
			Types: map[string]Type{
				"person": NatsType{
					InitFn:      func() any { return &Person{} },
					Name:        "Person",
					Description: "A person entity",
					DocPath:     personSchemaPath,
				},
			},
			Err: false,
		},
		"no-init": {
			Types: map[string]Type{
				"person": NatsType{
					InitFn: nil,
					Name:   "Person",
				},
			},
			Err: false, // Lazy loading doesn't validate during registration
		},
		"non-pointer": {
			Types: map[string]Type{
				"person": NatsType{
					InitFn: func() any { return Person{} },
					Name:   "Person",
				},
			},
			Err: false, // Lazy loading doesn't validate during registration
		},
		"not-serializable": {
			Types: map[string]Type{
				"not-serializable": NatsType{
					InitFn: func() any { return &NotSerializable{} },
					Name:   "NotSerializable",
				},
			},
			Err: false, // Lazy loading doesn't validate during registration
		},
		"invalid-schema-path": {
			Types: map[string]Type{
				"person": NatsType{
					InitFn:  func() any { return &Person{} },
					Name:    "Person",
					DocPath: "/non/existent/path.json",
				},
			},
			Err: true,
		},
		"invalid-schema-content": {
			Types: map[string]Type{
				"person": NatsType{
					InitFn:  func() any { return &Person{} },
					Name:    "Person",
					DocPath: invalidSchemaPath,
				},
			},
			Err: true,
		},
		"empty-name": {
			Types: map[string]Type{
				"": NatsType{
					InitFn: func() any { return &Person{} },
					Name:   "Person",
				},
			},
			Err: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewNatsRegistry(context.Background(), slog.New(slog.DiscardHandler), test.Types, codec.Default, nc)
			if err != nil && !test.Err {
				t.Errorf("unexpected error: %s", err)
			} else if err == nil && test.Err {
				t.Errorf("expected error")
			}
		})
	}
}

func TestNatsMarshalUnmarshal(t *testing.T) {
	is := testutil.NewIs(t)

	// Create test schema file
	tmpDir := t.TempDir()
	orderSchemaPath := filepath.Join(tmpDir, "order.json")
	orderSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"customer_id": {"type": "string"},
			"total": {"type": "number"},
			"items": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"product_id": {"type": "string"},
						"quantity": {"type": "integer"},
						"price": {"type": "number"}
					}
				}
			}
		},
		"required": ["id", "customer_id", "total", "items"]
	}`
	err := os.WriteFile(orderSchemaPath, []byte(orderSchema), 0644)
	is.NoErr(err)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	types := map[string]Type{
		"order": NatsType{
			InitFn: func() any {
				return &Order{}
			},
			Name:    "Order",
			DocPath: orderSchemaPath,
		},
	}

	// Only test with JSON and msgpack codecs since binary and protobuf have specific requirements
	compatibleCodecs := []codec.Codec{codec.JSON, codec.MsgPack}

	for _, c := range compatibleCodecs {
		t.Run(c.Name(), func(t *testing.T) {
			rt, err := NewNatsRegistry(context.Background(), slog.New(slog.DiscardHandler), types, c, nc)
			is.NoErr(err)

			t.Cleanup(func() {
				cleanupSchemasBucket(t, nc)
			})

			order1 := Order{
				ID:         "order-123",
				CustomerID: "customer-456",
				Total:      99.99,
				Items: []Item{
					{ProductID: "prod-1", Quantity: 2, Price: 29.99},
					{ProductID: "prod-2", Quantity: 1, Price: 40.01},
				},
			}

			// Test lookup
			name, err := rt.Lookup(&order1)
			is.NoErr(err)
			is.Equal(name, "order")

			// Test marshal
			b, err := rt.Marshal(&order1)
			is.NoErr(err)

			// Test init - now works with factory functions in memory
			x, err := rt.Init("order")
			is.NoErr(err)
			is.True(x != nil)
			_, ok := x.(*Order)
			is.True(ok)

			// Test unmarshal with pre-existing instance
			order2 := &Order{}
			err = rt.Unmarshal(b, order2)
			is.NoErr(err)
			is.Equal(order1.ID, order2.ID)
			is.Equal(order1.CustomerID, order2.CustomerID)
			is.Equal(order1.Total, order2.Total)
			is.Equal(len(order1.Items), len(order2.Items))
		})
	}
}

func TestNatsRegistryOperations(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	// Create test schema file
	tmpDir := t.TempDir()
	personSchemaPath := filepath.Join(tmpDir, "person.json")
	personSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"},
			"email": {"type": "string"}
		}
	}`
	err = os.WriteFile(personSchemaPath, []byte(personSchema), 0644)
	is.NoErr(err)

	types := map[string]Type{
		"person": NatsType{
			InitFn:  func() any { return &Person{} },
			Name:    "Person",
			DocPath: personSchemaPath,
		},
	}

	r, err := NewNatsRegistry(context.Background(), slog.New(slog.DiscardHandler), types, codec.Default, nc)
	is.NoErr(err)

	// Test Init - should work now that we keep factory functions in memory
	v, err := r.Init("person")
	is.NoErr(err)
	is.True(v != nil)
	_, ok := v.(*Person)
	is.True(ok)

	// Test Init with unknown type
	_, err = r.Init("unknown")
	is.True(err != nil)

	// Test Lookup with existing instance
	p := &Person{Name: "test", Age: 25}
	name, err := r.Lookup(p)
	is.NoErr(err)
	is.Equal(name, "person")

	// Test Lookup with unregistered type
	type UnknownType struct{}
	_, err = r.Lookup(&UnknownType{})
	is.True(err != nil)

	// Test Marshal/Unmarshal
	p1 := &Person{Name: "John Doe", Age: 30, Email: "john@example.com"}
	b, err := r.Marshal(p1)
	is.NoErr(err)

	p2 := &Person{}
	err = r.Unmarshal(b, p2)
	is.NoErr(err)
	is.Equal(p2.Name, "John Doe")
	is.Equal(p2.Age, 30)
	is.Equal(p2.Email, "john@example.com")

	// Test UnmarshalType - now works since Init is implemented
	v3, err := r.UnmarshalType(b, "person")
	is.NoErr(err)
	p3 := v3.(*Person)
	is.Equal(p3.Name, "John Doe")
	is.Equal(p3.Age, 30)
	is.Equal(p3.Email, "john@example.com")

	// Test Codec
	is.Equal(r.Codec(), codec.Default)
}

func TestNatsRegistryPointerNonPointer(t *testing.T) {
	is := testutil.NewIs(t)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	// Create minimal schema for testing
	tmpDir := t.TempDir()
	schemaPath := filepath.Join(tmpDir, "person.json")
	err = os.WriteFile(schemaPath, []byte(`{"type": "object"}`), 0644)
	is.NoErr(err)

	types := map[string]Type{
		"person": NatsType{
			InitFn:  func() any { return &Person{} },
			Name:    "Person",
			DocPath: schemaPath,
		},
	}

	r, err := NewNatsRegistry(context.Background(), slog.New(slog.DiscardHandler), types, codec.Default, nc)
	is.NoErr(err)

	// Create a value
	p := &Person{Name: "test", Age: 25}

	// Test that both pointer and value types can be looked up
	name, err := r.Lookup(p)
	is.NoErr(err)
	is.Equal(name, "person")

	// Test with value (dereference)
	name, err = r.Lookup(*p)
	is.NoErr(err)
	is.Equal(name, "person")
}

func TestNatsRegistryWithComplexSchema(t *testing.T) {
	is := testutil.NewIs(t)

	// Create a more complex schema with validation rules
	tmpDir := t.TempDir()
	schemaPath := filepath.Join(tmpDir, "complex.json")
	complexSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"name": {
				"type": "string",
				"minLength": 2,
				"maxLength": 100,
				"pattern": "^[a-zA-Z ]+$"
			},
			"age": {
				"type": "integer",
				"minimum": 18,
				"maximum": 100
			},
			"email": {
				"type": "string",
				"format": "email"
			}
		},
		"required": ["name", "age"],
		"additionalProperties": false
	}`
	err := os.WriteFile(schemaPath, []byte(complexSchema), 0644)
	is.NoErr(err)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	types := map[string]Type{
		"person": NatsType{
			InitFn:      func() any { return &Person{} },
			Name:        "Person",
			Description: "Person with validation",
			DocPath:     schemaPath,
		},
	}

	r, err := NewNatsRegistry(context.Background(), slog.New(slog.DiscardHandler), types, codec.Default, nc)
	is.NoErr(err)

	// Test with valid data
	p1 := &Person{Name: "John Doe", Age: 30, Email: "john@example.com"}
	b, err := r.Marshal(p1)
	is.NoErr(err)

	p2 := &Person{}
	err = r.Unmarshal(b, p2)
	is.NoErr(err)
	is.Equal(p1.Name, p2.Name)
}

func TestNatsRegistrySchemaValidation(t *testing.T) {
	is := testutil.NewIs(t)

	// Test that NATS registry can work without schema files
	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	// NatsType without DocPath should work
	types := map[string]Type{
		"person": NatsType{
			InitFn: func() any { return &Person{} },
			Name:   "Person",
			// No DocPath - should still work
		},
	}

	r, err := NewNatsRegistry(context.Background(), nil, types, codec.Default, nc)
	is.NoErr(err)

	// Init should work now with factory functions in memory
	v, err := r.Init("person")
	is.NoErr(err)
	is.True(v != nil)
	_, ok := v.(*Person)
	is.True(ok)

	p1 := &Person{Name: "Test", Age: 25}
	b, err := r.Marshal(p1)
	is.NoErr(err)

	p2 := &Person{}
	err = r.Unmarshal(b, p2)
	is.NoErr(err)
	is.Equal(p1.Name, p2.Name)
}

func BenchmarkNatsInit(b *testing.B) {
	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	// Clean up schemas bucket after benchmark
	b.Cleanup(func() {
		cleanupSchemasBucket(&testing.T{}, nc)
	})

	// Create minimal schema
	tmpDir := b.TempDir()
	schemaPath := filepath.Join(tmpDir, "person.json")
	_ = os.WriteFile(schemaPath, []byte(`{"type": "object"}`), 0644)

	r, _ := NewNatsRegistry(context.Background(), nil, map[string]Type{
		"person": NatsType{
			InitFn:  func() any { return &Person{} },
			Name:    "Person",
			DocPath: schemaPath,
		},
	}, codec.Default, nc)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = r.Init("person")
	}
}

func BenchmarkNatsLookup(b *testing.B) {
	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	// Clean up schemas bucket after benchmark
	b.Cleanup(func() {
		cleanupSchemasBucket(&testing.T{}, nc)
	})

	// Create minimal schema
	tmpDir := b.TempDir()
	schemaPath := filepath.Join(tmpDir, "person.json")
	_ = os.WriteFile(schemaPath, []byte(`{"type": "object"}`), 0644)

	r, _ := NewNatsRegistry(context.Background(), nil, map[string]Type{
		"person": NatsType{
			InitFn:  func() any { return &Person{} },
			Name:    "Person",
			DocPath: schemaPath,
		},
	}, codec.Default, nc)

	v := &Person{}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = r.Lookup(v)
	}
}

func BenchmarkNatsMarshalUnmarshal(b *testing.B) {
	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	// Clean up schemas bucket after benchmark
	b.Cleanup(func() {
		cleanupSchemasBucket(&testing.T{}, nc)
	})

	// Create minimal schema
	tmpDir := b.TempDir()
	schemaPath := filepath.Join(tmpDir, "person.json")
	_ = os.WriteFile(schemaPath, []byte(`{"type": "object"}`), 0644)

	r, _ := NewNatsRegistry(context.Background(), nil, map[string]Type{
		"person": NatsType{
			InitFn:  func() any { return &Person{} },
			Name:    "Person",
			DocPath: schemaPath,
		},
	}, codec.Default, nc)

	p := &Person{Name: "John Doe", Age: 30, Email: "john@example.com"}
	data, _ := r.Marshal(p)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		target := &Person{}
		_ = r.Unmarshal(data, target)
	}
}

func TestNatsRegistryDataValidation(t *testing.T) {
	is := testutil.NewIs(t)

	// Create test schema file with validation rules
	tmpDir := t.TempDir()
	personSchemaPath := filepath.Join(tmpDir, "person.json")
	personSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"name": {
				"type": "string",
				"minLength": 2,
				"maxLength": 50
			},
			"age": {
				"type": "integer",
				"minimum": 0,
				"maximum": 150
			},
			"email": {
				"type": "string",
				"format": "email"
			}
		},
		"required": ["name", "age"],
		"additionalProperties": false
	}`
	err := os.WriteFile(personSchemaPath, []byte(personSchema), 0644)
	is.NoErr(err)

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	is.NoErr(err)

	types := map[string]Type{
		"person": NatsType{
			InitFn:  func() any { return &Person{} },
			Name:    "Person",
			DocPath: personSchemaPath,
		},
	}

	r, err := NewNatsRegistry(context.Background(), slog.New(slog.DiscardHandler), types, codec.Default, nc)
	is.NoErr(err)

	// Test valid data passes validation
	validPerson := &Person{Name: "John Doe", Age: 30, Email: "john@example.com"}
	_, err = r.Marshal(validPerson)
	is.NoErr(err)

	// Test invalid age (negative)
	invalidAge := &Person{Name: "John", Age: -5}
	_, err = r.Marshal(invalidAge)
	is.True(err != nil) // Should fail validation
	if err != nil {
		is.True(strings.Contains(err.Error(), "validation failed"))
	}

	// Note: Go's int defaults to 0, which satisfies minimum:0 constraint
	// So we can't test "missing" age field this way

	// Test invalid email format
	invalidEmail := &Person{Name: "John", Age: 25, Email: "not-an-email"}
	_, err = r.Marshal(invalidEmail)
	is.True(err != nil) // Should fail validation
	if err != nil {
		is.True(strings.Contains(err.Error(), "validation failed"))
	}

	// Test name too short
	shortName := &Person{Name: "J", Age: 25}
	_, err = r.Marshal(shortName)
	is.True(err != nil) // Should fail validation
	if err != nil {
		is.True(strings.Contains(err.Error(), "validation failed"))
	}
}
