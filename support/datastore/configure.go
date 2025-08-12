package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

// DatastoreManifest represents the persisted configuration stored in the object store.
type DatastoreManifest struct {
	NetworkPassphrase string `json:"networkPassphrase"`
	Version           string `json:"version"`
	Compression       string `json:"compression"`
	LedgersPerFile    uint32 `json:"ledgersPerFile"`
	FilesPerPartition uint32 `json:"batchesPerPartition"`
}

// toDataStoreManifest transforms a user-provided config into a manifest for persistence.
func toDataStoreManifest(cfg DataStoreConfig) DatastoreManifest {
	return DatastoreManifest{
		NetworkPassphrase: cfg.NetworkPassphrase,
		Version:           Version,
		Compression:       cfg.Compression,
		LedgersPerFile:    cfg.Schema.LedgersPerFile,
		FilesPerPartition: cfg.Schema.FilesPerPartition,
	}
}

// compareManifests validates the equality of the expected and actual manifest values.
func compareManifests(expected, actual DatastoreManifest) error {
	if expected.NetworkPassphrase != "" && expected.NetworkPassphrase != actual.NetworkPassphrase {
		return fmt.Errorf("expected networkPassphrase=%q but found %q", expected.NetworkPassphrase, actual.NetworkPassphrase)
	}
	if expected.Version != "" && expected.Version != actual.Version {
		return fmt.Errorf("expected version=%q but found %q", expected.Version, actual.Version)
	}
	if expected.Compression != "" && expected.Compression != actual.Compression {
		return fmt.Errorf("expected compression=%q but found %q", expected.Compression, actual.Compression)
	}
	if expected.LedgersPerFile != 0 && expected.LedgersPerFile != actual.LedgersPerFile {
		return fmt.Errorf("expected ledgersPerFile=%d but found %d", expected.LedgersPerFile, actual.LedgersPerFile)
	}
	if expected.FilesPerPartition != 0 && expected.FilesPerPartition != actual.FilesPerPartition {
		return fmt.Errorf("expected filesPerPartition=%d but found %d", expected.FilesPerPartition, actual.FilesPerPartition)
	}
	return nil
}

// createManifest writes a new manifest to the datastore if it doesn't already exist.
func createManifest(ctx context.Context, dataStore DataStore, cfg DataStoreConfig) (DatastoreManifest, bool, error) {
	manifest := toDataStoreManifest(cfg)

	data, err := json.Marshal(manifest)
	if err != nil {
		return DatastoreManifest{}, false, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	ok, err := dataStore.PutFileIfNotExists(ctx, manifestFilename, bytes.NewReader(data), map[string]string{
		"Content-Type": "application/json",
	})
	if err != nil {
		return DatastoreManifest{}, false, fmt.Errorf("failed to write manifest file %q: %w", manifestFilename, err)
	}

	return manifest, ok, nil
}

func readManifest(ctx context.Context, dataStore DataStore, filename string) (DatastoreManifest, error) {
	reader, err := dataStore.GetFile(ctx, filename)
	if err != nil {
		return DatastoreManifest{}, fmt.Errorf("unable to open manifest file %q: %w", filename, err)
	}
	defer reader.Close()

	var manifest DatastoreManifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return DatastoreManifest{}, fmt.Errorf("invalid JSON in manifest file %q: %w", filename, err)
	}

	return manifest, nil
}

// PublishConfig ensures that a datastore manifest exists and matches the provided configuration.
// If the manifest is missing, it creates one. Returns the manifest, whether it was created, and any error encountered.
func PublishConfig(ctx context.Context, cfg DataStoreConfig) (DatastoreManifest, bool, error) {
	dataStore, err := NewDataStore(ctx, cfg)
	if err != nil {
		return DatastoreManifest{}, false, fmt.Errorf("failed to publish datastore config: %w", err)
	}
	defer dataStore.Close()

	manifest, err := readManifest(ctx, dataStore, manifestFilename)
	if err == nil {
		if err = compareManifests(toDataStoreManifest(cfg), manifest); err != nil {
			return manifest, false, fmt.Errorf("datastore config mismatch: %w", err)
		}
		return manifest, false, nil
	}

	createdManifest, created, writeErr := createManifest(ctx, dataStore, cfg)
	if writeErr != nil {
		return DatastoreManifest{}, false, writeErr
	}
	return createdManifest, created, nil
}

// LoadSchema reads the datastore manifest from the given DataStore and returns
// its schema configuration.
func LoadSchema(ctx context.Context, dataStore DataStore) (DataStoreSchema, error) {
	manifest, err := readManifest(ctx, dataStore, manifestFilename)
	if err != nil {
		return DataStoreSchema{}, err
	}

	return DataStoreSchema{
		LedgersPerFile:    manifest.LedgersPerFile,
		FilesPerPartition: manifest.FilesPerPartition,
	}, nil
}
