package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
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

type roundtripScenarioState struct {
	Scenario         string
	BaseTopic        string
	Topic            string
	Status           string
	Expected         int
	Snapshot         roundtripSnapshot
	ProducerErrors   int
	ConsumerErrors   int
	ValidationErrors int
	Duration         time.Duration
	ResultError      string
	ProducerSample   string
	ConsumerSample   string
	ValidationSample string
}

type roundtripTableRow struct {
	Status         string
	Scenario       string
	BaseTopic      string
	Topic          string
	UniqueExpected string
	Missing        string
	Duplicates     string
	OutOfOrder     string
	ProducerErrors string
	ConsumerErrors string
	ValidationErrs string
	Duration       string
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

			totalRuns := len(spec.Scenarios) * len(spec.Topics)
			parallelism = 1

			fmt.Fprintf(os.Stdout, "Roundtrip started: %d scenarios, %d base topics, parallelism=%d, total_runs=%d\n",
				len(spec.Scenarios), len(spec.Topics), parallelism, totalRuns)

			states := make(map[string]*roundtripScenarioState, totalRuns)

			completed := 0
			for _, scenario := range spec.Scenarios {
				messageCount := scenario.MessageCount
				if messageCount <= 0 {
					messageCount = defaultMessageCount
				}
				scenario := scenario
				for _, topic := range spec.Topics {
					topic := topic
					expected := scenario.Producers * messageCount
					stateKey := roundtripStateKey(scenario.Name, topic.Name)
					state := &roundtripScenarioState{
						Scenario:  scenario.Name,
						BaseTopic: topic.Name,
						Status:    "RUNNING",
						Expected:  expected,
					}
					states[stateKey] = state
					fmt.Fprintf(os.Stdout,
						"START scenario=%s base_topic=%s producers=%d consumers=%d subscription=%s expected=%d\n",
						scenario.Name,
						topic.Name,
						scenario.Producers,
						scenario.Consumers,
						scenario.SubscriptionType,
						expected,
					)

					result := runRoundtripScenario(client, scenario, topic.Name, messageCount, timeout, progressInterval, nil)
					completed++
					state.Topic = result.Topic
					state.Duration = result.Duration
					state.Expected = result.Stats.Expected
					state.Snapshot = roundtripSnapshot{
						Total:      result.Stats.TotalReceived,
						Unique:     result.Stats.UniqueReceived,
						Duplicates: result.Stats.Duplicates,
						OutOfOrder: result.Stats.OutOfOrder,
					}
					state.ProducerErrors = result.Stats.ProducerErrors
					state.ConsumerErrors = result.Stats.ConsumerErrors
					state.ValidationErrors = result.Stats.ValidationErrors
					state.ProducerSample = result.ProducerErrorSample
					state.ConsumerSample = result.ConsumerErrorSample
					state.ValidationSample = result.ValidationErrorSample
					if result.Err != nil {
						state.Status = "FAIL"
						state.ResultError = result.Err.Error()
					} else if hasRoundtripIssues(result.Stats) {
						state.Status = "FAIL"
					} else {
						state.Status = "OK"
					}

					printRoundtripProgress(states, completed, totalRuns)
				}
			}

			failures := printRoundtripSummary(states)
			if failures > 0 {
				logrus.Fatalf("roundtrip finished with %d failures", failures)
			}
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to HCL test spec")
	return cmd
}

type roundtripResult struct {
	Scenario              string
	BaseTopic             string
	Topic                 string
	Stats                 roundtripStats
	Err                   error
	Duration              time.Duration
	ProducerErrorSample   string
	ConsumerErrorSample   string
	ValidationErrorSample string
}

type roundtripProgress struct {
	Scenario         string
	BaseTopic        string
	Topic            string
	Expected         int
	Snapshot         roundtripSnapshot
	ProducerErrors   int
	ConsumerErrors   int
	ValidationErrors int
}

type roundtripErrorSample struct {
	mu     sync.Mutex
	sample string
}

func (s *roundtripErrorSample) record(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sample == "" {
		s.sample = fmt.Sprintf(format, args...)
	}
}

func (s *roundtripErrorSample) value() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sample
}

func runRoundtripScenario(
	client pulsar.Client,
	scenario roundtripScenario,
	baseTopic string,
	messageCount int,
	timeout time.Duration,
	progressInterval time.Duration,
	progressCh chan<- roundtripProgress,
) roundtripResult {
	start := time.Now()
	subscriptionType, err := parseSubscriptionType(scenario.SubscriptionType)
	if err != nil {
		return roundtripResult{
			Scenario:  scenario.Name,
			BaseTopic: baseTopic,
			Err:       err,
		}
	}

	if scenario.Producers <= 0 {
		return roundtripResult{Scenario: scenario.Name, BaseTopic: baseTopic, Err: fmt.Errorf("scenario %s must have producers > 0", scenario.Name)}
	}
	if scenario.Consumers <= 0 {
		return roundtripResult{Scenario: scenario.Name, BaseTopic: baseTopic, Err: fmt.Errorf("scenario %s must have consumers > 0", scenario.Name)}
	}

	testID := fmt.Sprintf("%d", time.Now().UnixNano())
	scenarioTopic := buildScenarioTopic(baseTopic, scenario.Name, testID)
	subscriptionName := sanitizeSubscription(fmt.Sprintf("rt-%s-%s", scenario.Name, testID))
	expected := scenario.Producers * messageCount

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	validator := newRoundtripValidator()

	producers := make([]pulsar.Producer, 0, scenario.Producers)
	consumers := make([]pulsar.Consumer, 0, scenario.Consumers)
	var producerErrors int64
	var consumerErrors int64
	var validationErrors int64
	var producerSample roundtripErrorSample
	var consumerSample roundtripErrorSample
	var validationSample roundtripErrorSample

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
				select {
				case progressCh <- roundtripProgress{
					Scenario:         scenario.Name,
					BaseTopic:        baseTopic,
					Topic:            scenarioTopic,
					Expected:         expected,
					Snapshot:         snap,
					ProducerErrors:   int(atomic.LoadInt64(&producerErrors)),
					ConsumerErrors:   int(atomic.LoadInt64(&consumerErrors)),
					ValidationErrors: int(atomic.LoadInt64(&validationErrors)),
				}:
				default:
				}
			}
		}
	}()

	for i := 0; i < scenario.Producers; i++ {
		producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: scenarioTopic})
		if err != nil {
			atomic.AddInt64(&producerErrors, 1)
			producerSample.record("producer=%d create: %v", i, err)
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
			Topic:            scenarioTopic,
			SubscriptionName: subscriptionName,
			Type:             subscriptionType,
		})
		if err != nil {
			atomic.AddInt64(&consumerErrors, 1)
			consumerSample.record("consumer=%d subscribe: %v", i, err)
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
			Scenario:  scenario.Name,
			BaseTopic: baseTopic,
			Topic:     scenarioTopic,
			Err:       fmt.Errorf("no consumers could be created"),
		}
	}
	if len(producers) == 0 {
		return roundtripResult{
			Scenario:  scenario.Name,
			BaseTopic: baseTopic,
			Topic:     scenarioTopic,
			Err:       fmt.Errorf("no producers could be created"),
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
					producerSample.record("producer=%s send seq=%d: %v", id, seq, err)
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
					atomic.AddInt64(&consumerErrors, 1)
					consumerSample.record("consumer=%d receive: %v", id, err)
					continue
				}

				props := msg.Properties()
				if props["rt_test_id"] != testID {
					cons.Ack(msg)
					continue
				}
				producerID := props["rt_producer_id"]
				seqRaw := props["rt_seq"]
				if producerID == "" || seqRaw == "" {
					atomic.AddInt64(&validationErrors, 1)
					validationSample.record("consumer=%d missing properties message_id=%s", id, msg.ID().String())
					cons.Ack(msg)
					continue
				}
				seq, err := strconv.Atoi(seqRaw)
				if err != nil {
					atomic.AddInt64(&validationErrors, 1)
					validationSample.record("consumer=%d bad seq=%s message_id=%s", id, seqRaw, msg.ID().String())
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
	stats.ValidationErrors = int(atomic.LoadInt64(&validationErrors))

	return roundtripResult{
		Scenario:              scenario.Name,
		BaseTopic:             baseTopic,
		Topic:                 scenarioTopic,
		Stats:                 stats,
		Duration:              time.Since(start),
		ProducerErrorSample:   producerSample.value(),
		ConsumerErrorSample:   consumerSample.value(),
		ValidationErrorSample: validationSample.value(),
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

func buildScenarioTopic(baseTopic, scenarioName, testID string) string {
	suffix := sanitizeSubscription(fmt.Sprintf("%s-%s", scenarioName, testID))
	lastSlash := strings.LastIndex(baseTopic, "/")
	if lastSlash == -1 || lastSlash == len(baseTopic)-1 {
		return fmt.Sprintf("%s-%s", baseTopic, suffix)
	}
	return fmt.Sprintf("%s-%s", baseTopic, suffix)
}

func roundtripStateKey(scenarioName, baseTopic string) string {
	return fmt.Sprintf("%s|%s", scenarioName, baseTopic)
}

func hasRoundtripIssues(stats roundtripStats) bool {
	return stats.Missing > 0 ||
		stats.Duplicates > 0 ||
		stats.OutOfOrder > 0 ||
		stats.ProducerErrors > 0 ||
		stats.ConsumerErrors > 0 ||
		stats.ValidationErrors > 0
}

func printRoundtripProgress(states map[string]*roundtripScenarioState, completed, total int) {
	keys := make([]string, 0, len(states))
	for key := range states {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	fmt.Fprintf(os.Stdout, "\nProgress: completed=%d/%d\n", completed, total)
	rows := buildRoundtripTableRows(states, keys)
	printRoundtripTable(rows)
}

func printRoundtripSummary(states map[string]*roundtripScenarioState) int {
	keys := make([]string, 0, len(states))
	for key := range states {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	fmt.Fprintln(os.Stdout, "\nSummary:")
	rows := buildRoundtripTableRows(states, keys)
	printRoundtripTable(rows)
	failures := 0
	for _, key := range keys {
		state := states[key]
		if state.Status == "FAIL" {
			failures++
		}
		if state.ResultError != "" {
			fmt.Fprintf(os.Stdout, "    error: %s\n", state.ResultError)
		}
		if state.ProducerSample != "" {
			fmt.Fprintf(os.Stdout, "    producer_error_sample: %s\n", state.ProducerSample)
		}
		if state.ConsumerSample != "" {
			fmt.Fprintf(os.Stdout, "    consumer_error_sample: %s\n", state.ConsumerSample)
		}
		if state.ValidationSample != "" {
			fmt.Fprintf(os.Stdout, "    validation_error_sample: %s\n", state.ValidationSample)
		}
	}
	return failures
}

func buildRoundtripTableRows(states map[string]*roundtripScenarioState, keys []string) []roundtripTableRow {
	rows := make([]roundtripTableRow, 0, len(keys))
	for _, key := range keys {
		state := states[key]
		missing := state.Expected - state.Snapshot.Unique
		if missing < 0 {
			missing = 0
		}
		topic := state.Topic
		if topic == "" {
			topic = "<pending>"
		}
		status := "RUN"
		if state.Status == "OK" {
			status = "OK"
		} else if state.Status == "FAIL" {
			status = "X"
		}
		duration := "-"
		if state.Duration > 0 {
			duration = state.Duration.Round(time.Millisecond).String()
		}
		rows = append(rows, roundtripTableRow{
			Status:         status,
			Scenario:       state.Scenario,
			BaseTopic:      state.BaseTopic,
			Topic:          topic,
			UniqueExpected: fmt.Sprintf("%d/%d", state.Snapshot.Unique, state.Expected),
			Missing:        fmt.Sprintf("%d", missing),
			Duplicates:     fmt.Sprintf("%d", state.Snapshot.Duplicates),
			OutOfOrder:     fmt.Sprintf("%d", state.Snapshot.OutOfOrder),
			ProducerErrors: fmt.Sprintf("%d", state.ProducerErrors),
			ConsumerErrors: fmt.Sprintf("%d", state.ConsumerErrors),
			ValidationErrs: fmt.Sprintf("%d", state.ValidationErrors),
			Duration:       duration,
		})
	}
	return rows
}

type roundtripTableColumn struct {
	Name  string
	Width int
}

func printRoundtripTable(rows []roundtripTableRow) {
	columns := []roundtripTableColumn{
		{Name: "status", Width: 6},
		{Name: "scenario", Width: 40},
		{Name: "base_topic", Width: 44},
		{Name: "topic", Width: 56},
		{Name: "unique/expected", Width: 15},
		{Name: "missing", Width: 7},
		{Name: "dup", Width: 5},
		{Name: "ooo", Width: 5},
		{Name: "prod_err", Width: 8},
		{Name: "cons_err", Width: 8},
		{Name: "val_err", Width: 7},
		{Name: "duration", Width: 9},
	}

	for idx := range columns {
		maxWidth := runeLen(columns[idx].Name)
		for _, row := range rows {
			value := roundtripColumnValue(row, columns[idx].Name)
			if length := runeLen(value); length > maxWidth {
				maxWidth = length
			}
		}
		if maxWidth > columns[idx].Width {
			maxWidth = columns[idx].Width
		}
		if maxWidth < runeLen(columns[idx].Name) {
			maxWidth = runeLen(columns[idx].Name)
		}
		columns[idx].Width = maxWidth
	}

	border := "+"
	for _, column := range columns {
		border += strings.Repeat("-", column.Width+2) + "+"
	}

	fmt.Fprintln(os.Stdout, border)
	fmt.Fprint(os.Stdout, "|")
	for _, column := range columns {
		fmt.Fprintf(os.Stdout, " %s |", padAndTrim(column.Name, column.Width))
	}
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, border)

	for _, row := range rows {
		fmt.Fprint(os.Stdout, "|")
		for _, column := range columns {
			value := roundtripColumnValue(row, column.Name)
			fmt.Fprintf(os.Stdout, " %s |", padAndTrim(value, column.Width))
		}
		fmt.Fprintln(os.Stdout)
	}
	fmt.Fprintln(os.Stdout, border)
}

func roundtripColumnValue(row roundtripTableRow, column string) string {
	switch column {
	case "status":
		return row.Status
	case "scenario":
		return row.Scenario
	case "base_topic":
		return row.BaseTopic
	case "topic":
		return row.Topic
	case "unique/expected":
		return row.UniqueExpected
	case "missing":
		return row.Missing
	case "dup":
		return row.Duplicates
	case "ooo":
		return row.OutOfOrder
	case "prod_err":
		return row.ProducerErrors
	case "cons_err":
		return row.ConsumerErrors
	case "val_err":
		return row.ValidationErrs
	case "duration":
		return row.Duration
	default:
		return ""
	}
}

func padAndTrim(value string, width int) string {
	trimmed := truncateRunes(value, width)
	if runeLen(trimmed) < width {
		return trimmed + strings.Repeat(" ", width-runeLen(trimmed))
	}
	return trimmed
}

func truncateRunes(value string, width int) string {
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	if width <= 1 {
		return string(runes[:width])
	}
	return string(runes[:width-1]) + "…"
}

func runeLen(value string) int {
	return len([]rune(value))
}
