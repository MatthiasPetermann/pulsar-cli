package cli

import (
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
)

// getClient creates a Pulsar client from environment configuration.
//
// Expected environment variables:
//   - PULSAR_URL (required)
//   - PULSAR_JWT (optional)
func getClient() pulsar.Client {
	url := os.Getenv("PULSAR_URL")
	if url == "" {
		logrus.Fatal("missing PULSAR_URL environment variable")
	}

	token := os.Getenv("PULSAR_JWT")
	return getClientWithOptions(url, token)
}

// getClientWithOptions creates a Pulsar client from explicit connection options.
func getClientWithOptions(url, token string) pulsar.Client {
	options := pulsar.ClientOptions{URL: url}
	if token != "" {
		options.Authentication = pulsar.NewAuthenticationToken(token)
	}

	client, err := pulsar.NewClient(options)
	if err != nil {
		logrus.Fatalf("could not create pulsar client: %v", err)
	}
	return client
}
