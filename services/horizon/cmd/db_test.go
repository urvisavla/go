package cmd

import (
	"testing"

	horizon "github.com/stellar/go/services/horizon/internal"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/ingest"
	"github.com/stellar/go/support/db/dbtest"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestDBCommandsTestSuite(t *testing.T) {
	dbCmdSuite := &DBCommandsTestSuite{}
	suite.Run(t, dbCmdSuite)
}

type DBCommandsTestSuite struct {
	suite.Suite
	dsn string
}

func (s *DBCommandsTestSuite) SetupSuite() {
	runDBReingestRangeFn = func([]history.LedgerRange, bool, uint,
		horizon.Config, ingest.StorageBackendConfig) error {
		return nil
	}

	newDB := dbtest.Postgres(s.T())
	s.dsn = newDB.DSN

	RootCmd.SetArgs([]string{
		"db", "migrate", "up", "--db-url", s.dsn})
	require.NoError(s.T(), RootCmd.Execute())
}

func (s *DBCommandsTestSuite) TestDefaultParallelJobSizeForBufferedBackend() {
	RootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.dsn,
		"--network", "testnet",
		"--parallel-workers", "2",
		"--ledgerbackend", "datastore",
		"--datastore-config", "../config.storagebackend.toml",
		"2",
		"10"})

	require.NoError(s.T(), dbReingestRangeCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(100))
}

func (s *DBCommandsTestSuite) TestDefaultParallelJobSizeForCaptiveBackend() {
	RootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.dsn,
		"--network", "testnet",
		"--stellar-core-binary-path", "/test/core/bin/path",
		"--parallel-workers", "2",
		"--ledgerbackend", "captive-core",
		"2",
		"10"})

	require.NoError(s.T(), RootCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(100_000))
}

func (s *DBCommandsTestSuite) TestUsesParallelJobSizeWhenSetForCaptive() {
	RootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.dsn,
		"--network", "testnet",
		"--stellar-core-binary-path", "/test/core/bin/path",
		"--parallel-workers", "2",
		"--parallel-job-size", "5",
		"--ledgerbackend", "captive-core",
		"2",
		"10"})

	require.NoError(s.T(), RootCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(5))
}

func (s *DBCommandsTestSuite) TestUsesParallelJobSizeWhenSetForBuffered() {
	RootCmd.SetArgs([]string{
		"db", "reingest", "range",
		"--db-url", s.dsn,
		"--network", "testnet",
		"--parallel-workers", "2",
		"--parallel-job-size", "5",
		"--ledgerbackend", "datastore",
		"--datastore-config", "../config.storagebackend.toml",
		"2",
		"10"})

	require.NoError(s.T(), RootCmd.Execute())
	require.Equal(s.T(), parallelJobSize, uint32(5))
}

func (s *DBCommandsTestSuite) TestDbReingestAndFillGapsCmds() {
	tests := []struct {
		name          string
		args          []string
		ledgerBackend ingest.LedgerBackendType
		expectError   bool
		errorMessage  string
	}{
		{
			name:        "default ledgerbackend",
			args:        []string{"1", "100"},
			expectError: false,
		},
		{
			name:        "captive-core ledgerbackend",
			args:        []string{"1", "100", "--ledgerbackend", "captive-core"},
			expectError: false,
		},
		{
			name:         "invalid ledgerbackend",
			args:         []string{"1", "100", "--ledgerbackend", "unknown"},
			expectError:  true,
			errorMessage: "invalid ledger backend: unknown, must be 'captive-core' or 'datastore'",
		},
		{
			name:         "datastore ledgerbackend without config",
			args:         []string{"1", "100", "--ledgerbackend", "datastore"},
			expectError:  true,
			errorMessage: "datastore config file is required for datastore backend type",
		},
		{
			name:         "datastore ledgerbackend missing config file",
			args:         []string{"1", "100", "--ledgerbackend", "datastore", "--datastore-config", "invalid.config.toml"},
			expectError:  true,
			errorMessage: "failed to load config file",
		},
		{
			name:        "datastore ledgerbackend",
			args:        []string{"1", "100", "--ledgerbackend", "datastore", "--datastore-config", "../config.storagebackend.toml"},
			expectError: false,
		},
	}

	commands := []struct {
		cmd  []string
		name string
	}{
		{[]string{"db", "reingest", "range"}, "TestDbReingestRangeCmd"},
		{[]string{"db", "fill-gaps"}, "TestDbFillGapsCmd"},
	}

	for _, command := range commands {
		for _, tt := range tests {
			s.T().Run(tt.name+"_"+command.name, func(t *testing.T) {
				args := append(command.cmd, tt.args...)
				RootCmd.SetArgs(append([]string{
					"--db-url", s.dsn,
					"--network", "testnet",
				}, args...))

				if tt.expectError {
					err := RootCmd.Execute()
					require.Error(t, err)
					require.Contains(t, err.Error(), tt.errorMessage)
				} else {
					require.NoError(t, RootCmd.Execute())
				}
			})
		}
	}
}
