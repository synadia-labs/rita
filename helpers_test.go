package rita

import "testing"

// storeHandle builds an unscoped handle the way the Manager constructors do,
// for unit tests that exercise subject construction without a server.
func storeHandle(name string) *EventStore {
	return &EventStore{name: name, stream: streamName(name), prefix: subjectRoot(name)}
}

// tenantHandle builds a tenant-scoped handle through the production Tenant()
// path so prefix derivation is exercised rather than restated.
func tenantHandle(t *testing.T, name, tenant string) *EventStore {
	t.Helper()
	base := storeHandle(name)
	base.tenantMode = true
	ten, err := base.Tenant(tenant)
	if err != nil {
		t.Fatal(err)
	}
	return ten
}
