package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConfigureDatastore(t *testing.T) {

	var defaultCfg = DataStoreConfig{
		Type: "test",
		Schema: DataStoreSchema{
			LedgersPerFile:    1000,
			FilesPerPartition: 10,
		},
		NetworkPassphrase: "passphrase",
		Compression:       "xyz",
	}

	var expectedManifest = DatastoreManifest{
		NetworkPassphrase: "passphrase",
		Version:           "1.0",
		Compression:       "xyz",
		LedgersPerFile:    1000,
		FilesPerPartition: 10,
	}
	configJSON, err := json.Marshal(expectedManifest)
	require.NoError(t, err)

	t.Run("creates new manifest", func(t *testing.T) {
		mockDataStore := new(MockDataStore)
		ctx := context.Background()

		mockDataStore.On("GetFile", ctx, manifestFilename).Return(nil, os.ErrNotExist).Once()
		mockDataStore.On("PutFileIfNotExists", ctx, manifestFilename, bytes.NewReader(configJSON),
			mock.Anything).Return(true, nil).Once()

		manifest, ok, err := PublishConfig(ctx, mockDataStore, defaultCfg)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, expectedManifest, manifest)

		mockDataStore.AssertExpectations(t)
	})

	t.Run("uses existing manifest", func(t *testing.T) {
		mockDataStore := new(MockDataStore)
		ctx := context.Background()

		mockDataStore.On("GetFile", ctx, manifestFilename).
			Return(io.NopCloser(bytes.NewReader(configJSON)), nil).Once()

		manifest, ok, err := PublishConfig(ctx, mockDataStore, defaultCfg)
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, expectedManifest, manifest)

		mockDataStore.AssertExpectations(t)
	})

	t.Run("returns error if PutFile fails", func(t *testing.T) {
		mockDataStore := new(MockDataStore)
		ctx := context.Background()

		mockDataStore.On("GetFile", ctx, manifestFilename).Return(nil, os.ErrNotExist).Once()
		mockDataStore.On("PutFileIfNotExists", ctx, manifestFilename, bytes.NewReader(configJSON), mock.Anything).
			Return(false, errors.New("boom")).Once()

		_, ok, err := PublishConfig(ctx, mockDataStore, defaultCfg)
		require.Error(t, err)
		require.False(t, ok)
		require.Contains(t, err.Error(), "boom")

		mockDataStore.AssertExpectations(t)
	})
}

func TestCompareManifests(t *testing.T) {
	with := func(base DatastoreManifest, modify func(*DatastoreManifest)) DatastoreManifest {
		copy := base
		modify(&copy)
		return copy
	}

	base := DatastoreManifest{
		NetworkPassphrase: "test-passphrase",
		Version:           "1.0",
		Compression:       "zstd",
		LedgersPerFile:    1000,
		FilesPerPartition: 10,
	}

	tests := []struct {
		name     string
		expected DatastoreManifest
		actual   DatastoreManifest
		wantErr  string
	}{
		{
			name:     "match",
			expected: base,
			actual:   base,
			wantErr:  "",
		},
		{
			name:     "network passphrase mismatch",
			expected: base,
			actual:   with(base, func(m *DatastoreManifest) { m.NetworkPassphrase = "wrong" }),
			wantErr:  `local="test-passphrase", datastore="wrong"`,
		},
		{
			name:     "version mismatch",
			expected: base,
			actual:   with(base, func(m *DatastoreManifest) { m.Version = "2.0" }),
			wantErr:  `local="1.0", datastore="2.0"`,
		},
		{
			name:     "compression mismatch",
			expected: base,
			actual:   with(base, func(m *DatastoreManifest) { m.Compression = "gzip" }),
			wantErr:  `local="zstd", datastore="gzip"`,
		},
		{
			name:     "ledgersPerFile mismatch",
			expected: base,
			actual:   with(base, func(m *DatastoreManifest) { m.LedgersPerFile = 500 }),
			wantErr:  `local=1000, datastore=500`,
		},
		{
			name:     "filesPerPartition mismatch",
			expected: base,
			actual:   with(base, func(m *DatastoreManifest) { m.FilesPerPartition = 5 }),
			wantErr:  `local=10, datastore=5`,
		},
		{
			name:     "empty expected manifest",
			expected: DatastoreManifest{},
			actual:   base,
			wantErr:  "", // Should not return an error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := compareManifests(tt.expected, tt.actual)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoadSchema(t *testing.T) {
	ctx := context.Background()
	defaultCfg := DataStoreConfig{
		Type:              "test",
		NetworkPassphrase: "passphrase",
		Schema: DataStoreSchema{
			LedgersPerFile:    1000,
			FilesPerPartition: 10,
		},
		Compression: "gzip",
	}
	validManifest := DatastoreManifest{
		NetworkPassphrase: "passphrase",
		Version:           "1.0",
		Compression:       "gzip",
		LedgersPerFile:    1000,
		FilesPerPartition: 10,
	}

	validManifestBytes, err := json.Marshal(validManifest)
	require.NoError(t, err)

	// Manifest file exists and is valid (happy path)
	t.Run("Manifest found and valid", func(t *testing.T) {
		mockOS := new(MockDataStore)
		mockOS.On("GetFile", ctx, manifestFilename).Return(io.NopCloser(bytes.NewReader(validManifestBytes)), nil).Once()
		mockOS.On("ListFilePaths", ctx, "", 2).Return(nil, nil)
		schema, err := LoadSchema(ctx, mockOS, defaultCfg)
		require.NoError(t, err)
		require.NotNil(t, schema)
		require.Equal(t, uint32(1000), schema.LedgersPerFile)
		require.Equal(t, uint32(10), schema.FilesPerPartition)
		mockOS.AssertExpectations(t)
	})

	// Manifest file not found (backward compatibility), fallback to config
	t.Run("Manifest not found", func(t *testing.T) {
		mockOS := new(MockDataStore)
		mockOS.On("GetFile", ctx, manifestFilename).Return(nil, os.ErrNotExist).Once()
		mockOS.On("ListFilePaths", ctx, "", 2).Return(nil, nil)

		schema, err := LoadSchema(ctx, mockOS, defaultCfg)
		require.NoError(t, err)
		require.NotNil(t, schema)
		require.Equal(t, uint32(1000), schema.LedgersPerFile)
		require.Equal(t, uint32(10), schema.FilesPerPartition)
		mockOS.AssertExpectations(t)
	})

	t.Run("Manifest found but invalid JSON", func(t *testing.T) {
		mockOS := new(MockDataStore)
		mockOS.On("GetFile", ctx, manifestFilename).Return(io.NopCloser(bytes.NewReader([]byte(`{"invalid": "json"`))), nil).Once()
		mockOS.On("ListFilePaths", ctx, "", 2).Return(nil, nil)

		schema, err := LoadSchema(ctx, mockOS, defaultCfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid JSON in manifest file")
		require.EqualValues(t, DataStoreSchema{}, schema)
		mockOS.AssertExpectations(t)
	})

	// Manifest found but config is different (validation fails)
	t.Run("Manifest found but validation fails", func(t *testing.T) {
		mockOS := new(MockDataStore)
		invalidManifestBytes, err := json.Marshal(DatastoreManifest{
			LedgersPerFile:    500,
			FilesPerPartition: 5,
		})
		require.NoError(t, err)

		mockOS.On("GetFile", ctx, manifestFilename).Return(io.NopCloser(bytes.NewReader(invalidManifestBytes)), nil).Once()
		mockOS.On("ListFilePaths", ctx, "", 2).Return(nil, nil)

		schema, err := LoadSchema(ctx, mockOS, defaultCfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "datastore config mismatch")
		require.EqualValues(t, DataStoreSchema{}, schema)
		mockOS.AssertExpectations(t)
	})
}
