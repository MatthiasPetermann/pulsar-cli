package cli

import (
	"context"
	"os"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// consumerCmd reads messages through a subscription and acknowledges them.
func consumerCmd() *cobra.Command {
	var topic, subscription string
	var useRegex bool
	var subscriptionType string
	var subscriptionInitialPosition string

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

			subscriptionInitialPosition = strings.ToLower(subscriptionInitialPosition)
			var pulsarInitialPosition pulsar.SubscriptionInitialPosition
			switch subscriptionInitialPosition {
			case "earliest":
				pulsarInitialPosition = pulsar.SubscriptionPositionEarliest
			case "latest":
				pulsarInitialPosition = pulsar.SubscriptionPositionLatest
			default:
				logrus.Fatalf("invalid subscription initial position %q (expected earliest or latest)", subscriptionInitialPosition)
			}

			client := getClient()
			defer client.Close()

			opts := pulsar.ConsumerOptions{
				SubscriptionName:            subscription,
				Type:                        pulsarSubscriptionType,
				SubscriptionInitialPosition: pulsarInitialPosition,
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
	cmd.Flags().StringVar(&subscriptionInitialPosition, "subscription-initial-position", "latest", "Initial position for new subscriptions: earliest or latest")

	return cmd
}
