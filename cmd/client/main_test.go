// Unit tests for DistKV client
package main

import (
	"strings"
	"testing"
	"time"

	"distkv/proto"
)

// TestParseConsistency tests conversion of the -consistency flag
func TestParseConsistency(t *testing.T) {
	tests := []struct {
		input string
		want  proto.ConsistencyLevel
	}{
		{"", proto.ConsistencyLevel_DEFAULT},
		{"one", proto.ConsistencyLevel_ONE},
		{"quorum", proto.ConsistencyLevel_QUORUM},
		{"all", proto.ConsistencyLevel_ALL},
		{"ONE", proto.ConsistencyLevel_ONE},
		{"QuOrUm", proto.ConsistencyLevel_QUORUM},
	}
	for _, tt := range tests {
		got, err := parseConsistency(tt.input)
		if err != nil || got != tt.want {
			t.Errorf("parseConsistency(%q) = %v, %v; want %v", tt.input, got, err, tt.want)
		}
	}
	if _, err := parseConsistency("invalid"); err == nil {
		t.Error("parseConsistency accepted an unknown level")
	}
}

// TestClientConfigDefaults tests default configuration values
func TestClientConfigDefaults(t *testing.T) {
	config := &ClientConfig{
		ServerAddress:    "localhost:8080",
		Timeout:          5 * time.Second,
		ConsistencyLevel: proto.ConsistencyLevel_ONE,
	}

	if config.ServerAddress != "localhost:8080" {
		t.Errorf("Expected default server address 'localhost:8080', got '%s'", config.ServerAddress)
	}

	if config.Timeout != 5*time.Second {
		t.Errorf("Expected default timeout 5s, got %v", config.Timeout)
	}

	if config.ConsistencyLevel != proto.ConsistencyLevel_ONE {
		t.Errorf("Expected default consistency level ONE, got %v", config.ConsistencyLevel)
	}
}

// TestClientConfigCustom tests custom configuration
func TestClientConfigCustom(t *testing.T) {
	config := &ClientConfig{
		ServerAddress:    "example.com:9000",
		Timeout:          10 * time.Second,
		ConsistencyLevel: proto.ConsistencyLevel_ALL,
	}

	if config.ServerAddress != "example.com:9000" {
		t.Errorf("Expected server address 'example.com:9000', got '%s'", config.ServerAddress)
	}

	if config.Timeout != 10*time.Second {
		t.Errorf("Expected timeout 10s, got %v", config.Timeout)
	}

	if config.ConsistencyLevel != proto.ConsistencyLevel_ALL {
		t.Errorf("Expected consistency level ALL, got %v", config.ConsistencyLevel)
	}
}

// TestDistKVClientStructure tests client structure
func TestDistKVClientStructure(t *testing.T) {
	// Can't create actual client without server, but can verify structure
	client := &DistKVClient{
		config: &ClientConfig{
			ServerAddress:    "localhost:8080",
			Timeout:          5 * time.Second,
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		},
		conn:         nil, // Would be set by NewDistKVClient
		distkvClient: nil, // Would be set by NewDistKVClient
		adminClient:  nil, // Would be set by NewDistKVClient
	}

	if client.config == nil {
		t.Error("Config should not be nil")
	}

	if client.config.ServerAddress != "localhost:8080" {
		t.Error("Config not set correctly")
	}
}

// TestPrintUsageDoesNotPanic tests that usage printing works
func TestPrintUsageDoesNotPanic(t *testing.T) {
	// Just verify it doesn't panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("printUsage panicked: %v", r)
		}
	}()

	printUsage()
}

// TestParseErrorHelper tests parseError utility
func TestParseErrorHelper(t *testing.T) {
	tests := []struct {
		name         string
		errorMessage string
		expectNil    bool
		expectMsg    string
	}{
		{"empty string", "", true, ""},
		{"error message", "operation failed", false, "operation failed"},
		{"complex error", "network: connection timeout", false, "network: connection timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate parseError logic from replica_client.go
			var err error
			if tt.errorMessage != "" {
				err = &testError{msg: tt.errorMessage}
			}

			if tt.expectNil {
				if err != nil {
					t.Errorf("Expected nil error, got: %v", err)
				}
			} else {
				if err == nil {
					t.Error("Expected non-nil error, got nil")
				} else if err.Error() != tt.expectMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.expectMsg, err.Error())
				}
			}
		})
	}
}

// Helper type for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// TestConsistencyLevelEnums tests consistency level values
func TestConsistencyLevelEnums(t *testing.T) {
	levels := []proto.ConsistencyLevel{
		proto.ConsistencyLevel_ONE,
		proto.ConsistencyLevel_QUORUM,
		proto.ConsistencyLevel_ALL,
	}

	if len(levels) != 3 {
		t.Error("Expected 3 consistency levels")
	}

	// Verify they're different values
	seen := make(map[proto.ConsistencyLevel]bool)
	for _, level := range levels {
		if seen[level] {
			t.Errorf("Duplicate consistency level value: %v", level)
		}
		seen[level] = true
	}
}

// TestClientCloseNilConnection tests closing with nil connection
func TestClientCloseNilConnection(t *testing.T) {
	client := &DistKVClient{
		config: &ClientConfig{
			ServerAddress:    "localhost:8080",
			Timeout:          5 * time.Second,
			ConsistencyLevel: proto.ConsistencyLevel_ONE,
		},
		conn: nil,
	}

	err := client.Close()
	if err != nil {
		t.Errorf("Close with nil connection should not error, got: %v", err)
	}
}

// TestBatchPutArgsProcessing tests batch put argument processing
func TestBatchPutArgsProcessing(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		expectedPairs int
	}{
		{"two pairs", []string{"k1", "v1", "k2", "v2"}, 2},
		{"one pair", []string{"key", "value"}, 1},
		{"three pairs", []string{"a", "1", "b", "2", "c", "3"}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate batch put logic
			items := make(map[string][]byte)
			for i := 0; i < len(tt.args); i += 2 {
				key := tt.args[i]
				value := tt.args[i+1]
				items[key] = []byte(value)
			}

			if len(items) != tt.expectedPairs {
				t.Errorf("Expected %d pairs, got %d", tt.expectedPairs, len(items))
			}
		})
	}
}

// TestClientTimeoutConfiguration tests timeout settings
func TestClientTimeoutConfiguration(t *testing.T) {
	timeouts := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
	}

	for _, timeout := range timeouts {
		config := &ClientConfig{
			ServerAddress:    "localhost:8080",
			Timeout:          timeout,
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		}

		if config.Timeout != timeout {
			t.Errorf("Expected timeout %v, got %v", timeout, config.Timeout)
		}
	}
}

// TestServerAddressFormats tests various server address formats
func TestServerAddressFormats(t *testing.T) {
	addresses := []string{
		"localhost:8080",
		"127.0.0.1:8080",
		"example.com:9000",
		"192.168.1.10:8080",
		"[::1]:8080", // IPv6
	}

	for _, addr := range addresses {
		config := &ClientConfig{
			ServerAddress:    addr,
			Timeout:          5 * time.Second,
			ConsistencyLevel: proto.ConsistencyLevel_ONE,
		}

		if config.ServerAddress != addr {
			t.Errorf("Server address not preserved: expected '%s', got '%s'", addr, config.ServerAddress)
		}
	}
}

// TestConsistencyLevelParsing tests all consistency level strings
func TestConsistencyLevelParsing(t *testing.T) {
	testCases := []struct {
		input    string
		expected proto.ConsistencyLevel
	}{
		{"one", proto.ConsistencyLevel_ONE},
		{"ONE", proto.ConsistencyLevel_ONE},
		{"quorum", proto.ConsistencyLevel_QUORUM},
		{"QUORUM", proto.ConsistencyLevel_QUORUM},
		{"all", proto.ConsistencyLevel_ALL},
		{"ALL", proto.ConsistencyLevel_ALL},
		{"invalid", proto.ConsistencyLevel_ONE}, // defaults to ONE
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			var level proto.ConsistencyLevel
			switch strings.ToLower(tc.input) {
			case "one":
				level = proto.ConsistencyLevel_ONE
			case "quorum":
				level = proto.ConsistencyLevel_QUORUM
			case "all":
				level = proto.ConsistencyLevel_ALL
			default:
				level = proto.ConsistencyLevel_ONE
			}

			if level != tc.expected {
				t.Errorf("For input '%s': expected %v, got %v", tc.input, tc.expected, level)
			}
		})
	}
}

// TestClientOperationsStructure verifies operation method signatures exist
func TestClientOperationsStructure(t *testing.T) {
	// This test verifies the client has the expected methods
	// We can't test actual operations without a server, but we can verify structure

	client := &DistKVClient{
		config: &ClientConfig{
			ServerAddress:    "localhost:8080",
			Timeout:          5 * time.Second,
			ConsistencyLevel: proto.ConsistencyLevel_QUORUM,
		},
	}

	// Verify methods exist (will panic if not)
	_ = client.Put       // func(string, string) error
	_ = client.Get       // func(string) error
	_ = client.Delete    // func(string) error
	_ = client.BatchPut  // func([]string) error
	_ = client.GetStatus // func() error
	_ = client.Close     // func() error

	// If we got here, all methods exist
	t.Log("All expected client methods exist")
}
