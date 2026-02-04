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

func consumerCmd() *cobra.Command {
	var topic, subscription string
	var useRegex bool
	var subscriptionType string

	cmd := &cobra.Command{
		Use:   "consumer",
		Short: "Consume messages from a Pulsar topic or a topic pattern using a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if subscription == "" {
				logrus.Fatal("subscription is required")
			}
			if topic == "" {
				logrus.Fatal("topic or regex pattern is required")
			}

			subscriptionType = strings.ToLower(subscriptionType)
			var pulsarSubscriptionType pulsar.SubscriptionType
			switch subscriptionType {
			case "exclusive":
				pulsarSubscriptionType = pulsar.Exclusive
			case "shared":
				pulsarSubscriptionType = pulsar.Shared
			case "failover":
				pulsarSubscriptionType = pulsar.Failover
			default:
				logrus.Fatalf("invalid subscription type %q (expected exclusive, shared, or failover)", subscriptionType)
			}

			client := getClient()
			defer client.Close()

			opts := pulsar.ConsumerOptions{
				SubscriptionName: subscription,
				Type:             pulsarSubscriptionType,
			}

			if useRegex {
				opts.TopicsPattern = topic
				logrus.Infof("Consuming from topic pattern %s with subscription %s ...", topic, subscription)
			} else {
				opts.Topic = topic
				logrus.Infof("Consuming from topic %s with subscription %s ...", topic, subscription)
			}

			consumer, err := client.Subscribe(opts)
			if err != nil {
				logrus.Fatalf("failed to create consumer: %v", err)
			}
			defer consumer.Close()

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

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic or regex pattern to consume from")
	cmd.Flags().StringVarP(&subscription, "subscription", "s", "", "Subscription name")
	cmd.Flags().BoolVar(&useRegex, "regex", false, "Treat the topic as a regex pattern (subscribe to multiple matching topics)")
	cmd.Flags().StringVar(&subscriptionType, "subscription-type", "shared", "Subscription type: exclusive, shared, or failover")

	return cmd
}

func producerCmd() *cobra.Command {
	var topic string
	var propertyFlags []string
	var filePath string
	var delimiter string
	var enableChunking bool
	var enableBatching bool

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
				Topic:          topic,
				DisableBatching: !enableBatching,
				EnableChunking: enableChunking, // 🔹 enable when flag is set
			})
			if err != nil {
				logrus.Fatalf("failed to create producer: %v", err)
			}
			defer producer.Close()

			// File mode
			if filePath != "" {
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
						"topic":          topic,
						"file":           filePath,
						"time":           time.Now(),
						"properties":     props,
						"chunkingActive": enableChunking,
						"batchingActive": enableBatching,
					}).Info("sent file as message")
				}
				return
			}

			// STDIN mode
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
								"topic":          topic,
								"time":           time.Now(),
								"properties":     props,
								"chunkingActive": enableChunking,
								"batchingActive": enableBatching,
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
	cmd.Flags().BoolVarP(&enableChunking, "enable-chunking", "c", false, "Enable native Pulsar message chunking for large payloads")
	cmd.Flags().BoolVarP(&enableBatching, "enable-batching", "b", false, "Enable native Pulsar message batching")

	return cmd
}
