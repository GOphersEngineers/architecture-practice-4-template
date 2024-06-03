package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const baseAddress = "http://balancer:8090"

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	server1, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
	require.NoError(t, err, "Failed to get response from server1")

	server1Header := server1.Header.Get("lb-from")
	assert.Equal(t, "server1:8080", server1Header, "Expected lb-from header to be 'server1:8080'")

	db, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=gophersengineers", baseAddress))
	require.NoError(t, err, "Failed to get response from database")

	var body Response
	err = json.NewDecoder(db.Body).Decode(&body)
	require.NoError(t, err, "Failed to decode response body")

	assert.Equal(t, "gophersengineers", body.Key, "Expected key to be 'gophersengineers'")
	assert.NotEmpty(t, body.Value, "Expected value not to be empty")
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	for i := 0; i < b.N; i++ {
		_, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		require.NoError(b, err, "Failed to get response from server")
	}
}
