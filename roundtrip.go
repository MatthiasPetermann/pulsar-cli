package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type roundtripSpec struct {
	Global    roundtripGlobal     `hcl:"global,block"`
	Topics    []roundtripTopic    `hcl:"topic,block"`
	Scenarios []roundtripScenario `hcl:"scenario,block"`
}

type roundtripGlobal struct {
	BrokerURL        string `hcl:"broker_url,optional"`
	JWT              string `hcl:"jwt,optional"`
	Parallelism      int    `hcl:"parallelism,optional"`
	MessageCount     int    `hcl:"message_count,optional"`
	Timeout          string `hcl:"timeout,optional"`
	ProgressInterval string `hcl:"progress_interval,optional"`
}

type roundtripTopic struct {
	Name string `hcl:"name,label"`
}

type roundtripScenario struct {
	Name             string `hcl:"name,label"`
	Producers        int    `hcl:"producers"`
	Consumers        int    `hcl:"consumers"`
	SubscriptionType string `hcl:"subscription_type"`
	MessageCount     int    `hcl:"message_count,optional"`
}

type roundtripStats struct {
	Expected         int
	TotalReceived    int
	UniqueReceived   int
	Duplicates       int
	OutOfOrder       int
	Missing          int
	ProducerErrors   int
	ConsumerErrors   int
	ValidationErrors int
}

type roundtripSnapshot struct {
	Total      int
	Unique     int
	Duplicates int
	OutOfOrder int
}

type roundtripValidator struct {
	mu         sync.Mutex
	seen       map[string]map[int]struct{}
	lastSeq    map[string]int
	total      int
	unique     int
	duplicates int
	outOfOrder int
}

func newRoundtripValidator() *roundtripValidator {
	return &roundtripValidator{
		seen:    make(map[string]map[int]struct{}),
		lastSeq: make(map[string]int),
	}
}

func (v *roundtripValidator) record(producerID string, seq int) (bool, int, int, int) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.total++
	if v.seen[producerID] == nil {
		v.seen[producerID] = make(map[int]struct{})
	}
	if _, exists := v.seen[producerID][seq]; exists {
		v.duplicates++
		return false, v.total, v.unique, v.duplicates
	}

	if last, ok := v.lastSeq[producerID]; ok {
		if seq != last+1 {
			v.outOfOrder++
		}
	}
	v.lastSeq[producerID] = seq
	v.seen[producerID][seq] = struct{}{}
	v.unique++
	return true, v.total, v.unique, v.duplicates
}

func (v *roundtripValidator) snapshot() roundtripSnapshot {
	v.mu.Lock()
	defer v.mu.Unlock()
	return roundtripSnapshot{
		Total:      v.total,
		Unique:     v.unique,
		Duplicates: v.duplicates,
		OutOfOrder: v.outOfOrder,
	}
}

func (v *roundtripValidator) stats(expected int) roundtripStats {
	v.mu.Lock()
	defer v.mu.Unlock()
	missing := expected - v.unique
	if missing < 0 {
		missing = 0
	}
	return roundtripStats{
		Expected:       expected,
		TotalReceived:  v.total,
		UniqueReceived: v.unique,
		Duplicates:     v.duplicates,
		OutOfOrder:     v.outOfOrder,
		Missing:        missing,
	}
}

func parseRoundtripSpec(path string) (roundtripSpec, error) {
	file, err := os.Open(path)
	if err != nil {
		return roundtripSpec{}, err
	}
	defer file.Close()

	var spec roundtripSpec
	scanner := bufio.NewScanner(file)
	var currentBlock string

	for scanner.Scan() {
		line := stripHCLComments(scanner.Text())
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if line == "}" {
			currentBlock = ""
			continue
		}

		if strings.HasPrefix(line, "global") && strings.Contains(line, "{") {
			currentBlock = "global"
			continue
		}

		if strings.HasPrefix(line, "topic") && strings.Contains(line, "{") {
			label, err := parseHCLLabel(line)
			if err != nil {
				return roundtripSpec{}, err
			}
			spec.Topics = append(spec.Topics, roundtripTopic{Name: label})
			currentBlock = "topic"
			continue
		}

		if strings.HasPrefix(line, "scenario") && strings.Contains(line, "{") {
			label, err := parseHCLLabel(line)
			if err != nil {
				return roundtripSpec{}, err
			}
			spec.Scenarios = append(spec.Scenarios, roundtripScenario{Name: label})
			currentBlock = "scenario"
			continue
		}

		if currentBlock == "" {
			return roundtripSpec{}, fmt.Errorf("unexpected content outside block: %s", line)
		}

		key, rawValue, err := parseHCLAssignment(line)
		if err != nil {
			return roundtripSpec{}, err
		}

		switch currentBlock {
		case "global":
			if err := applyGlobalValue(&spec.Global, key, rawValue); err != nil {
				return roundtripSpec{}, err
			}
		case "scenario":
			if len(spec.Scenarios) == 0 {
				return roundtripSpec{}, fmt.Errorf("scenario block missing label")
			}
			last := &spec.Scenarios[len(spec.Scenarios)-1]
			if err := applyScenarioValue(last, key, rawValue); err != nil {
				return roundtripSpec{}, err
			}
		case "topic":
			return roundtripSpec{}, fmt.Errorf("topic blocks do not accept attributes: %s", line)
		default:
			return roundtripSpec{}, fmt.Errorf("unknown block type %s", currentBlock)
		}
	}

	if err := scanner.Err(); err != nil {
		return roundtripSpec{}, err
	}

	return spec, nil
}

func stripHCLComments(line string) string {
	var builder strings.Builder
	inQuote := false
	for i := 0; i < len(line); i++ {
		char := line[i]
		if char == '"' {
			inQuote = !inQuote
		}
		if !inQuote {
			if char == '#' {
				break
			}
			if char == '/' && i+1 < len(line) && line[i+1] == '/' {
				break
			}
		}
		builder.WriteByte(char)
	}
	return builder.String()
}

func parseHCLLabel(line string) (string, error) {
	start := strings.Index(line, "\"")
	end := strings.LastIndex(line, "\"")
	if start == -1 || end == -1 || end <= start {
		return "", fmt.Errorf("missing block label: %s", line)
	}
	return line[start+1 : end], nil
}

func parseHCLAssignment(line string) (string, string, error) {
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid assignment: %s", line)
	}
	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	if key == "" || value == "" {
		return "", "", fmt.Errorf("invalid assignment: %s", line)
	}
	return key, value, nil
}

func parseHCLString(value string) (string, error) {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") && len(value) >= 2 {
		return strings.Trim(value, "\""), nil
	}
	return "", fmt.Errorf("expected quoted string, got %q", value)
}

func parseHCLInt(value string) (int, error) {
	value = strings.TrimSpace(value)
	return strconv.Atoi(value)
}

func applyGlobalValue(global *roundtripGlobal, key string, value string) error {
	switch key {
	case "broker_url":
		parsed, err := parseHCLString(value)
		if err != nil {
			return err
		}
		global.BrokerURL = parsed
	case "jwt":
		parsed, err := parseHCLString(value)
		if err != nil {
			return err
		}
		global.JWT = parsed
	case "parallelism":
		parsed, err := parseHCLInt(value)
		if err != nil {
			return err
		}
		global.Parallelism = parsed
	case "message_count":
		parsed, err := parseHCLInt(value)
		if err != nil {
			return err
		}
		global.MessageCount = parsed
	case "timeout":
		parsed, err := parseHCLString(value)
		if err != nil {
			return err
		}
		global.Timeout = parsed
	case "progress_interval":
		parsed, err := parseHCLString(value)
		if err != nil {
			return err
		}
		global.ProgressInterval = parsed
	default:
		return fmt.Errorf("unknown global key: %s", key)
	}
	return nil
}

func applyScenarioValue(scenario *roundtripScenario, key string, value string) error {
	switch key {
	case "producers":
		parsed, err := parseHCLInt(value)
		if err != nil {
			return err
		}
		scenario.Producers = parsed
	case "consumers":
		parsed, err := parseHCLInt(value)
		if err != nil {
			return err
		}
		scenario.Consumers = parsed
	case "subscription_type":
		parsed, err := parseHCLString(value)
		if err != nil {
			return err
		}
		scenario.SubscriptionType = parsed
	case "message_count":
		parsed, err := parseHCLInt(value)
		if err != nil {
			return err
		}
		scenario.MessageCount = parsed
	default:
		return fmt.Errorf("unknown scenario key: %s", key)
	}
	return nil
}

func roundtripCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "roundtrip",
		Short: "Run roundtrip validation against a Pulsar broker using an HCL spec",
		Run: func(cmd *cobra.Command, args []string) {
			if configPath == "" {
				logrus.Fatal("config path is required")
			}

			spec, err := parseRoundtripSpec(configPath)
			if err != nil {
				logrus.Fatalf("failed to parse HCL config: %v", err)
			}

			if len(spec.Topics) == 0 {
				logrus.Fatal("config must define at least one topic block")
			}
			if len(spec.Scenarios) == 0 {
				logrus.Fatal("config must define at least one scenario block")
			}

			parallelism := spec.Global.Parallelism
			if parallelism <= 0 {
				parallelism = 4
			}
			defaultMessageCount := spec.Global.MessageCount
			if defaultMessageCount <= 0 {
				defaultMessageCount = 100
			}

			timeout := 45 * time.Second
			if spec.Global.Timeout != "" {
				parsed, err := time.ParseDuration(spec.Global.Timeout)
				if err != nil {
					logrus.Fatalf("invalid timeout duration: %v", err)
				}
				timeout = parsed
			}

			progressInterval := 2 * time.Second
			if spec.Global.ProgressInterval != "" {
				parsed, err := time.ParseDuration(spec.Global.ProgressInterval)
				if err != nil {
					logrus.Fatalf("invalid progress_interval duration: %v", err)
				}
				progressInterval = parsed
			}

			brokerURL := spec.Global.BrokerURL
			if brokerURL == "" {
				brokerURL = os.Getenv("PULSAR_URL")
			}
			if brokerURL == "" {
				logrus.Fatal("missing broker_url in config or PULSAR_URL env var")
			}
			jwt := spec.Global.JWT
			if jwt == "" {
				jwt = os.Getenv("PULSAR_JWT")
			}

			client := getClientWithOptions(brokerURL, jwt)
			defer client.Close()

			fmt.Fprintf(os.Stdout, "Roundtrip started: %d scenarios, %d topics, parallelism=%d\n",
				len(spec.Scenarios), len(spec.Topics), parallelism)

			var wg sync.WaitGroup
			sem := make(chan struct{}, parallelism)
			results := make(chan roundtripResult, len(spec.Scenarios)*len(spec.Topics))

			for _, scenario := range spec.Scenarios {
				messageCount := scenario.MessageCount
				if messageCount <= 0 {
					messageCount = defaultMessageCount
				}
				scenario := scenario
				for _, topic := range spec.Topics {
					topic := topic
					wg.Add(1)
					go func() {
						defer wg.Done()
						sem <- struct{}{}
						defer func() { <-sem }()
						results <- runRoundtripScenario(client, scenario, topic.Name, messageCount, timeout, progressInterval)
					}()
				}
			}

			wg.Wait()
			close(results)

			var failures int
			for result := range results {
				if result.Err != nil {
					failures++
					fmt.Fprintf(os.Stdout, "ERROR scenario=%s topic=%s: %v\n", result.Scenario, result.Topic, result.Err)
					continue
				}
				stats := result.Stats
				status := "OK"
				if stats.Missing > 0 || stats.Duplicates > 0 || stats.OutOfOrder > 0 || stats.ConsumerErrors > 0 || stats.ProducerErrors > 0 {
					status = "WARN"
				}
				fmt.Fprintf(os.Stdout,
					"Result %s scenario=%s topic=%s duration=%s expected=%d unique=%d total=%d dup=%d out_of_order=%d missing=%d producer_errors=%d consumer_errors=%d\n",
					status,
					result.Scenario,
					result.Topic,
					result.Duration.Round(time.Millisecond),
					stats.Expected,
					stats.UniqueReceived,
					stats.TotalReceived,
					stats.Duplicates,
					stats.OutOfOrder,
					stats.Missing,
					stats.ProducerErrors,
					stats.ConsumerErrors,
				)
			}

			if failures > 0 {
				logrus.Fatalf("roundtrip finished with %d failures", failures)
			}
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to HCL test spec")
	return cmd
}

type roundtripResult struct {
	Scenario string
	Topic    string
	Stats    roundtripStats
	Err      error
	Duration time.Duration
}

func runRoundtripScenario(client pulsar.Client, scenario roundtripScenario, topic string, messageCount int, timeout time.Duration, progressInterval time.Duration) roundtripResult {
	start := time.Now()
	subscriptionType, err := parseSubscriptionType(scenario.SubscriptionType)
	if err != nil {
		return roundtripResult{
			Scenario: scenario.Name,
			Topic:    topic,
			Err:      err,
		}
	}

	if scenario.Producers <= 0 {
		return roundtripResult{Scenario: scenario.Name, Topic: topic, Err: fmt.Errorf("scenario %s must have producers > 0", scenario.Name)}
	}
	if scenario.Consumers <= 0 {
		return roundtripResult{Scenario: scenario.Name, Topic: topic, Err: fmt.Errorf("scenario %s must have consumers > 0", scenario.Name)}
	}

	testID := fmt.Sprintf("%d", time.Now().UnixNano())
	subscriptionName := sanitizeSubscription(fmt.Sprintf("rt-%s-%s-%s", scenario.Name, topic, testID))
	expected := scenario.Producers * messageCount

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	validator := newRoundtripValidator()

	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap := validator.snapshot()
				fmt.Fprintf(os.Stdout,
					"Progress scenario=%s topic=%s unique=%d/%d total=%d dup=%d out_of_order=%d\n",
					scenario.Name,
					topic,
					snap.Unique,
					expected,
					snap.Total,
					snap.Duplicates,
					snap.OutOfOrder,
				)
			}
		}
	}()

	producers := make([]pulsar.Producer, 0, scenario.Producers)
	consumers := make([]pulsar.Consumer, 0, scenario.Consumers)
	var producerErrors int64
	var consumerErrors int64

	for i := 0; i < scenario.Producers; i++ {
		producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
		if err != nil {
			atomic.AddInt64(&producerErrors, 1)
			fmt.Fprintf(os.Stdout, "ERROR scenario=%s topic=%s producer=%d: %v\n", scenario.Name, topic, i, err)
			continue
		}
		producers = append(producers, producer)
	}
	defer func() {
		for _, producer := range producers {
			producer.Close()
		}
	}()

	for i := 0; i < scenario.Consumers; i++ {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: subscriptionName,
			Type:             subscriptionType,
		})
		if err != nil {
			atomic.AddInt64(&consumerErrors, 1)
			fmt.Fprintf(os.Stdout, "ERROR scenario=%s topic=%s consumer=%d: %v\n", scenario.Name, topic, i, err)
			continue
		}
		consumers = append(consumers, consumer)
	}
	defer func() {
		for _, consumer := range consumers {
			consumer.Close()
		}
	}()

	if len(consumers) == 0 {
		return roundtripResult{
			Scenario: scenario.Name,
			Topic:    topic,
			Err:      fmt.Errorf("no consumers could be created"),
		}
	}
	if len(producers) == 0 {
		return roundtripResult{
			Scenario: scenario.Name,
			Topic:    topic,
			Err:      fmt.Errorf("no producers could be created"),
		}
	}

	var produceWG sync.WaitGroup
	for idx, producer := range producers {
		produceWG.Add(1)
		producerID := fmt.Sprintf("producer-%d", idx)
		go func(prod pulsar.Producer, id string) {
			defer produceWG.Done()
			for seq := 1; seq <= messageCount; seq++ {
				msg := &pulsar.ProducerMessage{
					Payload: []byte(fmt.Sprintf("producer=%s seq=%d", id, seq)),
					Properties: map[string]string{
						"rt_test_id":     testID,
						"rt_producer_id": id,
						"rt_seq":         strconv.Itoa(seq),
						"rt_scenario":    scenario.Name,
					},
				}
				if _, err := prod.Send(ctx, msg); err != nil {
					atomic.AddInt64(&producerErrors, 1)
					fmt.Fprintf(os.Stdout, "ERROR scenario=%s topic=%s producer=%s seq=%d: %v\n", scenario.Name, topic, id, seq, err)
				}
			}
		}(producer, producerID)
	}

	var consumeWG sync.WaitGroup
	for idx, consumer := range consumers {
		consumeWG.Add(1)
		consumerID := idx
		go func(cons pulsar.Consumer, id int) {
			defer consumeWG.Done()
			for {
				msg, err := cons.Receive(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					fmt.Fprintf(os.Stdout, "ERROR scenario=%s topic=%s consumer=%d receive: %v\n", scenario.Name, topic, id, err)
					continue
				}

				props := msg.Properties()
				producerID := props["rt_producer_id"]
				seqRaw := props["rt_seq"]
				if producerID == "" || seqRaw == "" {
					fmt.Fprintf(os.Stdout, "ERROR scenario=%s topic=%s consumer=%d missing_properties message_id=%s\n", scenario.Name, topic, id, msg.ID().Serialize())
					cons.Ack(msg)
					continue
				}
				seq, err := strconv.Atoi(seqRaw)
				if err != nil {
					fmt.Fprintf(os.Stdout, "ERROR scenario=%s topic=%s consumer=%d bad_seq=%s message_id=%s\n", scenario.Name, topic, id, seqRaw, msg.ID().Serialize())
					cons.Ack(msg)
					continue
				}

				_, _, unique, _ := validator.record(producerID, seq)
				if unique >= expected {
					cons.Ack(msg)
					cancel()
					return
				}
				cons.Ack(msg)
			}
		}(consumer, consumerID)
	}

	produceWG.Wait()
	consumeWG.Wait()

	<-progressDone

	stats := validator.stats(expected)
	stats.ProducerErrors = int(atomic.LoadInt64(&producerErrors))
	stats.ConsumerErrors = int(atomic.LoadInt64(&consumerErrors))

	return roundtripResult{
		Scenario: scenario.Name,
		Topic:    topic,
		Stats:    stats,
		Duration: time.Since(start),
	}
}

func parseSubscriptionType(subscriptionType string) (pulsar.SubscriptionType, error) {
	switch strings.ToLower(subscriptionType) {
	case "exclusive":
		return pulsar.Exclusive, nil
	case "shared":
		return pulsar.Shared, nil
	case "failover":
		return pulsar.Failover, nil
	default:
		return pulsar.Shared, fmt.Errorf("invalid subscription type %q (expected exclusive, shared, or failover)", subscriptionType)
	}
}

func sanitizeSubscription(value string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		default:
			return '-'
		}
	}, value)
}
