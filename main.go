package main

import (
	"bufio"
	"context"
	"os"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func main() {
	// Colored logs to stderr (Unix best practice).
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors:   true,  // force colors even if no TTY
		FullTimestamp: true,  // readable timestamps
	})
	logrus.SetOutput(os.Stderr)
	logrus.SetLevel(logrus.InfoLevel)

	rootCmd := &cobra.Command{
		Use:   "pulsar-cli",
		Short: "CLI Pulsar client supporting reader, consumer, and producer",
	}

	rootCmd.AddCommand(readerCmd())
	rootCmd.AddCommand(consumerCmd())
	rootCmd.AddCommand(producerCmd())

	if err := rootCmd.Execute(); err != nil {
		logrus.Fatal(err)
	}
}

// Build Pulsar client from env vars PULSAR_URL and optional PULSAR_JWT.
func getClient() pulsar.Client {
	url := os.Getenv("PULSAR_URL")
	if url == "" {
		logrus.Fatal("missing PULSAR_URL environment variable")
	}

	token := os.Getenv("PULSAR_JWT")
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

// Reader: log metadata (and properties) to stderr; write payload to stdout.
func readerCmd() *cobra.Command {
	var topic string

	cmd := &cobra.Command{
		Use:   "reader",
		Short: "Read messages from a Pulsar topic",
		Run: func(cmd *cobra.Command, args []string) {
			if topic == "" {
				logrus.Fatal("topic is required")
			}

			client := getClient()
			defer client.Close()

			reader, err := client.CreateReader(pulsar.ReaderOptions{
				Topic:          topic,
				StartMessageID: pulsar.LatestMessageID(),
			})
			if err != nil {
				logrus.Fatalf("failed to create reader: %v", err)
			}
			defer reader.Close()

			logrus.Infof("Reading from topic %s ...", topic)

			for {
				msg, err := reader.Next(context.Background())
				if err != nil {
					logrus.Errorf("read error: %v", err)
					continue
				}

				// Properties + Metadata -> stderr
				props := msg.Properties()
				logFields := logrus.Fields{
					"topic":     msg.Topic(),
					"msgID":     msg.ID().Serialize(),
					"publishAt": msg.PublishTime(),
				}
				if len(props) > 0 {
					logFields["properties"] = props
				}

				logrus.WithFields(logFields).Info("received message")

				// Payload -> stdout (newline-terminated)
				_, _ = os.Stdout.Write(append(msg.Payload(), '\n'))
			}
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic to read from")
	return cmd
}

// Consumer: log metadata (and properties) to stderr; write payload to stdout.
func consumerCmd() *cobra.Command {
	var topic, subscription string

	cmd := &cobra.Command{
		Use:   "consumer",
		Short: "Consume messages from a Pulsar topic using a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if topic == "" || subscription == "" {
				logrus.Fatal("topic and subscription are required")
			}

			client := getClient()
			defer client.Close()

			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				Topic:            topic,
				SubscriptionName: subscription,
				Type:             pulsar.Shared,
			})
			if err != nil {
				logrus.Fatalf("failed to create consumer: %v", err)
			}
			defer consumer.Close()

			logrus.Infof("Consuming from topic %s with subscription %s ...", topic, subscription)

			for {
				msg, err := consumer.Receive(context.Background())
				if err != nil {
					logrus.Errorf("receive error: %v", err)
					continue
				}

				// Properties + Metadata -> stderr
				props := msg.Properties()
				logFields := logrus.Fields{
					"topic":     msg.Topic(),
					"msgID":     msg.ID().Serialize(),
					"publishAt": msg.PublishTime(),
				}
				if len(props) > 0 {
					logFields["properties"] = props
				}

				logrus.WithFields(logFields).Info("received message")

				// Payload -> stdout
				_, _ = os.Stdout.Write(append(msg.Payload(), '\n'))

				consumer.Ack(msg)
			}
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic to consume from")
	cmd.Flags().StringVarP(&subscription, "subscription", "s", "", "Subscription name")
	return cmd
}

// Producer: read lines from stdin and send; log only metadata to stderr.
func producerCmd() *cobra.Command {
	var topic string
	var propertyFlags []string

	cmd := &cobra.Command{
		Use:   "producer",
		Short: "Produce messages to a Pulsar topic (reads from stdin)",
		Run: func(cmd *cobra.Command, args []string) {
			if topic == "" {
				logrus.Fatal("topic is required")
			}

			// Parse --property flags into map[string]string
			props := map[string]string{}
			for _, kv := range propertyFlags {
				parts := strings.SplitN(kv, "=", 2)
				if len(parts) != 2 {
					logrus.Fatalf("invalid property format: %s (expected key=value)", kv)
				}
				props[parts[0]] = parts[1]
			}

			client := getClient()
			defer client.Close()

			producer, err := client.CreateProducer(pulsar.ProducerOptions{
				Topic: topic,
			})
			if err != nil {
				logrus.Fatalf("failed to create producer: %v", err)
			}
			defer producer.Close()

			scanner := bufio.NewScanner(os.Stdin)
			logrus.Infof("Producing messages to topic %s (Ctrl+D to quit)", topic)

			for scanner.Scan() {
				text := scanner.Text()
				msg := &pulsar.ProducerMessage{
					Payload:    []byte(text),
					Properties: props,
				}

				if _, err := producer.Send(context.Background(), msg); err != nil {
					logrus.Errorf("send error: %v", err)
				} else {
					// Metadata + properties
					logFields := logrus.Fields{
						"topic":      topic,
						"time":       time.Now(),
						"properties": props,
					}
					logrus.WithFields(logFields).Info("sent message")
				}
			}
			if err := scanner.Err(); err != nil {
				logrus.Errorf("stdin error: %v", err)
			}
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic to produce to")
	cmd.Flags().StringArrayVarP(&propertyFlags, "property", "p", nil, "Message property in key=value format (repeatable)")

	return cmd
}
