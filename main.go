package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
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
		ForceColors:   true,
		FullTimestamp: true,
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

// Reader: unchanged
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
				props := msg.Properties()
				fields := logrus.Fields{
					"topic":     msg.Topic(),
					"msgID":     msg.ID().Serialize(),
					"publishAt": msg.PublishTime(),
				}
				if len(props) > 0 {
					fields["properties"] = props
				}
				logrus.WithFields(fields).Info("received message")
				_, _ = os.Stdout.Write(append(msg.Payload(), '\n'))
			}
		},
	}
	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic to read from")
	return cmd
}

// Consumer: unchanged
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
				props := msg.Properties()
				fields := logrus.Fields{
					"topic":     msg.Topic(),
					"msgID":     msg.ID().Serialize(),
					"publishAt": msg.PublishTime(),
				}
				if len(props) > 0 {
					fields["properties"] = props
				}
				logrus.WithFields(fields).Info("received message")
				_, _ = os.Stdout.Write(append(msg.Payload(), '\n'))
				consumer.Ack(msg)
			}
		},
	}
	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic to consume from")
	cmd.Flags().StringVarP(&subscription, "subscription", "s", "", "Subscription name")
	return cmd
}

// Producer: now supports --file and --delimiter
func producerCmd() *cobra.Command {
	var topic string
	var propertyFlags []string
	var filePath string
	var delimiter string

	cmd := &cobra.Command{
		Use:   "producer",
		Short: "Produce messages to a Pulsar topic (reads from stdin or file)",
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

			// Read payload
			if filePath != "" {
				// --- File Mode: send one message with entire file content ---
				data, err := os.ReadFile(filePath)
				if err != nil {
					logrus.Fatalf("failed to read file: %v", err)
				}
				msg := &pulsar.ProducerMessage{
					Payload:    data,
					Properties: props,
				}
				if _, err := producer.Send(context.Background(), msg); err != nil {
					logrus.Errorf("send error: %v", err)
				} else {
					logrus.WithFields(logrus.Fields{
						"topic":      topic,
						"file":       filePath,
						"time":       time.Now(),
						"properties": props,
					}).Info("sent file as single message")
				}
				return
			}

			// --- STDIN Mode ---
			if delimiter == "" {
				delimiter = "\n"
			}
			logrus.Infof("Producing messages to topic %s (Ctrl+D to quit)", topic)

			reader := bufio.NewReader(os.Stdin)
			var buffer bytes.Buffer

			for {
				line, err := reader.ReadString('\n')
				if err != nil && err != io.EOF {
					logrus.Errorf("stdin read error: %v", err)
					break
				}
				buffer.WriteString(line)

				// Check for delimiter or EOF
				if strings.HasSuffix(buffer.String(), delimiter) || err == io.EOF {
					payload := strings.TrimSuffix(buffer.String(), delimiter)
					buffer.Reset()

					if len(payload) > 0 {
						msg := &pulsar.ProducerMessage{
							Payload:    []byte(payload),
							Properties: props,
						}
						if _, err := producer.Send(context.Background(), msg); err != nil {
							logrus.Errorf("send error: %v", err)
						} else {
							logrus.WithFields(logrus.Fields{
								"topic":      topic,
								"time":       time.Now(),
								"properties": props,
							}).Info("sent message")
						}
					}
				}
				if err == io.EOF {
					break
				}
			}
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic to produce to")
	cmd.Flags().StringArrayVarP(&propertyFlags, "property", "p", nil, "Message property in key=value format (repeatable)")
	cmd.Flags().StringVarP(&filePath, "file", "f", "", "Read payload from file (sends one message per file)")
	cmd.Flags().StringVarP(&delimiter, "delimiter", "d", "", "Custom message delimiter for stdin mode (default: newline)")

	return cmd
}
