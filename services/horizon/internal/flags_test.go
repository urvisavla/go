package horizon

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCaptiveCoreConfigFromNetwork(t *testing.T) {

	tests := []struct {
		name                  string
		config                Config
		networkPassphrase     string
		historyArchiveURLs    []string
		stellarCoreBinaryPath string
		errStr                string
	}{
		{
			name:               "testnet default config",
			config:             Config{Network: StellarTestnet},
			networkPassphrase:  testnetConf.networkPassphrase,
			historyArchiveURLs: testnetConf.historyArchiveURLs,
		},
		{
			name:               "pubnet default config",
			config:             Config{Network: StellarPubnet},
			networkPassphrase:  pubnetConf.networkPassphrase,
			historyArchiveURLs: pubnetConf.historyArchiveURLs,
		},
		{
			name: "unknown network specified",
			config: Config{Network: "unknown",
				NetworkPassphrase:  "",
				HistoryArchiveURLs: []string{},
			},
			errStr: "no default configuration found for network unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := createCaptiveCoreConfigFromNetwork(&tt.config)
			if tt.errStr == "" {
				assert.NoError(t, e)
				assert.Equal(t, tt.networkPassphrase, tt.config.NetworkPassphrase)
				assert.Equal(t, tt.historyArchiveURLs, tt.config.HistoryArchiveURLs)
			} else {
				assert.Equal(t, tt.errStr, e.Error())
			}
		})
	}
}

func TestCreateCaptiveCoreConfigFromParameters(t *testing.T) {
	var errorMsgConfig = "%s must be set"
	tests := []struct {
		name               string
		config             Config
		networkPassphrase  string
		historyArchiveURLs []string
		errStr             string
	}{
		{
			name: "no network specified",
			config: Config{
				NetworkPassphrase:     "NetworkPassphrase",
				HistoryArchiveURLs:    []string{"HistoryArchiveURLs"},
				CaptiveCoreBinaryPath: "stellarCoreBinaryPath",
			},
			networkPassphrase:  "NetworkPassphrase",
			historyArchiveURLs: []string{"HistoryArchiveURLs"},
		},
		{
			name: "no network specified, stellar-core binary path not specified",
			config: Config{
				NetworkPassphrase:  "NetworkPassphrase",
				HistoryArchiveURLs: []string{"HistoryArchiveURLs"},
			},
			networkPassphrase:  "NetworkPassphrase",
			historyArchiveURLs: []string{"HistoryArchiveURLs"},
		},
		{
			name: "no network specified; passphrase not supplied",
			config: Config{
				HistoryArchiveURLs: []string{"HistoryArchiveURLs"},
			},
			errStr: fmt.Sprintf(errorMsgConfig, NetworkPassphraseFlagName),
		},
		{
			name: "no network specified; history archive urls not supplied",
			config: Config{
				NetworkPassphrase: "NetworkPassphrase",
			},
			errStr: fmt.Sprintf(errorMsgConfig, HistoryArchiveURLsFlagName),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := createCaptiveCoreConfigFromParameters(&tt.config)
			if tt.errStr == "" {
				assert.NoError(t, e)
				assert.Equal(t, tt.networkPassphrase, tt.config.NetworkPassphrase)
				assert.Equal(t, tt.historyArchiveURLs, tt.config.HistoryArchiveURLs)
			} else {
				require.Error(t, e)
				assert.Equal(t, tt.errStr, e.Error())
			}
		})
	}
}

func TestValidateNetworkConfigParameter(t *testing.T) {
	var invalidConfigErrMsg = "invalid configuration: You cannot specify --%s with the '%s' network parameter"
	var invalidConfigErrMsgIngestion = invalidConfigErrMsg + ". By default, --%s is true when the network parameter is specified"
	tests := []struct {
		name   string
		config Config
		errStr string
	}{
		{
			name: "testnet validation; ingest false",
			config: Config{
				Ingest:                     false,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarTestnet,
			},
			errStr: fmt.Sprintf(invalidConfigErrMsgIngestion, IngestFlagName, StellarTestnet, IngestFlagName),
		},
		{
			name: "pubnet validation; ingest false",
			config: Config{
				Ingest:                     false,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarPubnet,
			},
			errStr: fmt.Sprintf(invalidConfigErrMsgIngestion, IngestFlagName, StellarPubnet, IngestFlagName),
		},
		{
			name: "testnet validation; enable-captive-core-ingestion false",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: false,
				Network:                    StellarTestnet,
			},
			errStr: fmt.Sprintf(invalidConfigErrMsgIngestion, EnableCaptiveCoreIngestionFlagName,
				StellarTestnet, EnableCaptiveCoreIngestionFlagName),
		},
		{
			name: "pubnet validation; enable-captive-core-ingestion false",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: false,
				Network:                    StellarPubnet,
			},
			errStr: fmt.Sprintf(invalidConfigErrMsgIngestion, EnableCaptiveCoreIngestionFlagName,
				StellarPubnet, EnableCaptiveCoreIngestionFlagName),
		},
		{
			name: "testnet validation; history archive urls supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarTestnet,
				HistoryArchiveURLs:         []string{"network history archive urls supplied"},
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, HistoryArchiveURLsFlagName, StellarTestnet),
		},
		{
			name: "pubnet validation; history archive urls supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarPubnet,
				HistoryArchiveURLs:         []string{"network history archive urls supplied"},
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, HistoryArchiveURLsFlagName, StellarPubnet),
		},
		{
			name: "testnet validation; network passphrase supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarTestnet,
				NetworkPassphrase:          "network passphrase supplied",
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, NetworkPassphraseFlagName, StellarTestnet),
		},
		{
			name: "pubnet validation; network passphrase supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarPubnet,
				NetworkPassphrase:          "pubnet network passphrase supplied",
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, NetworkPassphraseFlagName, StellarPubnet),
		},
		{
			name: "testnet validation; captive-core-config-path-name supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarTestnet,
				CaptiveCoreConfigPath:      "CaptiveCoreConfigPath",
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, CaptiveCoreConfigPathName, StellarTestnet),
		},
		{
			name: "pubnet validation; captive-core-config-path-name supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarPubnet,
				CaptiveCoreConfigPath:      "CaptiveCoreConfigPath",
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, CaptiveCoreConfigPathName, StellarPubnet),
		},
		{
			name: "testnet validation; stellar-core-binary-path supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarTestnet,
				CaptiveCoreBinaryPath:      "StellarCoreBinaryPathName",
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, StellarCoreBinaryPathName, StellarTestnet),
		},
		{
			name: "pubnet validation; stellar-core-binary-path supplied",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarPubnet,
				CaptiveCoreBinaryPath:      "StellarCoreBinaryPathName",
			},
			errStr: fmt.Sprintf(invalidConfigErrMsg, StellarCoreBinaryPathName, StellarPubnet),
		},
		{
			name: "testnet validation success",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarTestnet,
			},
		},
		{
			name: "pubnet validation success",
			config: Config{
				Ingest:                     true,
				EnableCaptiveCoreIngestion: true,
				Network:                    StellarPubnet,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validateNetworkConfigParameter(&tt.config)
			if tt.errStr == "" {
				assert.NoError(t, e)
			} else {
				require.Error(t, e)
				assert.Equal(t, tt.errStr, e.Error())
			}
		})
	}
}
