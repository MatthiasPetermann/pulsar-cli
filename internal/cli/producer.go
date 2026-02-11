package cli

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

// producerCmd publishes messages from a file or stdin.
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

			// Parse --property flags into key/value message properties.
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
				Topic:           topic,
				DisableBatching: !enableBatching,
				EnableChunking:  enableChunking,
			})
			if err != nil {
				logrus.Fatalf("failed to create producer: %v", err)
			}
			defer producer.Close()

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
