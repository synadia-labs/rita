package testutil

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestRemoveStoreDir_OutlastsWritesThatFinishAfterShutdown(t *testing.T) {
	dir, err := os.MkdirTemp("", "rita-store-*")
	if err != nil {
		t.Fatal(err)
	}
	obs := filepath.Join(dir, "obs", "consumer")
	if err := os.MkdirAll(obs, 0o700); err != nil {
		t.Fatal(err)
	}

	stop := time.Now().Add(150 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; time.Now().Before(stop); i++ {
			_ = os.WriteFile(filepath.Join(obs, "o.dat."+strconv.Itoa(i)), []byte("state"), 0o600)
		}
	}()

	err = removeStoreDir(dir, 2*time.Second)
	<-done
	if err != nil {
		t.Fatalf("remove store dir: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("store dir still exists: %v", err)
	}
}
