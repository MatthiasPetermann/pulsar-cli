package cli

import (
	"context"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// readerCmd streams messages from a topic and writes payloads to stdout.
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
