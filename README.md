# Pulsar CLI

A lightweight **command-line client for Apache Pulsar** written in Go.
It supports **producing**, **reading**, and **consuming** messages from Pulsar topics — with **colored logs on `stderr`** and **message payloads on `stdout`** for easy piping.

---

## ✨ Features

- **Reader** — read messages from a topic (no subscription)
- **Consumer** — consume messages with a subscription
- **Regex topic patterns** (`--regex`) to match multiple topics
- **Producer** — publish messages from `stdin` **or a file**
- **Custom delimiters** for `stdin` mode (e.g. `--delimiter="\n\n"`)
- **Message properties** via `--property key=value`
- **Chunking** (`--enable-chunking`) for large payloads
- **Batching** (`--enable-batching`) for throughput
- **Colored logs** with `logrus` on `stderr`
- **Payloads printed to `stdout`** for UNIX-style piping (`jq`, `grep`, etc.)
- **Environment-based configuration** (`PULSAR_URL`, `PULSAR_JWT`)
- **Static, portable builds** (no dependencies)

---

## 📦 Installation

### 🧰 Build from Source

Clone and build:

```bash
git clone https://forge.ext.d2ux.net/Atlas/pulsar-cli
cd pulsar-cli

go mod tidy
make build
```

Binary will be at `./pulsar-cli`.

For static builds:

```bash
make build-linux
make build-macos
make build-windows
```

---

## ⚙️ Environment Variables

| Variable       | Description                             | Example                              |
|----------------|-----------------------------------------|--------------------------------------|
| `PULSAR_URL`   | Pulsar broker URL                       | `pulsar://localhost:6650`            |
| `PULSAR_JWT`   | (Optional) JWT token for authentication | `eyJhbGciOiJIUzI1NiIsInR5cCI6...`    |

---

## 🧠 Usage

### Reader

Read messages from a topic **without subscription**:

```bash
pulsar-cli reader -t "my-topic"
```

---

### Consumer

Consume messages with a subscription:

```bash
pulsar-cli consumer -t "my-topic" -s "my-subscription"
```

#### Consume using a Regex pattern

You can subscribe to **multiple matching topics** using a regex pattern with `--regex`:

```bash
pulsar-cli consumer -t "persistent://public/default/my-topic-.*" -s "multi-sub" --regex
```

This will automatically receive messages from all topics matching the pattern, such as:
- `persistent://public/default/my-topic-1`
- `persistent://public/default/my-topic-foo`
- `persistent://public/default/my-topic-bar`

---

### Producer

Publish messages (from `stdin` or a file) to a topic.

#### From stdin

```bash
echo "Hello, Pulsar!" | pulsar-cli producer -t "my-topic"
```

#### With custom delimiter

```bash
pulsar-cli producer -t "my-topic" -d "\n\n"
```

#### From a file

```bash
pulsar-cli producer -t "my-topic" -f ./data.json
```

#### With message properties

```bash
echo "hi" | pulsar-cli producer -t "my-topic" -p app=demo -p env=dev
```

#### Enable chunking

```bash
cat large.json | pulsar-cli producer -t "my-topic" --enable-chunking
```

#### Enable batching

```bash
seq 1 10000 | pulsar-cli producer -t "my-topic" --enable-batching
```

---

## 🧾 Command Reference

### Reader

| Flag | Description |
|------|--------------|
| `-t, --topic` | Topic to read from |

---

### Consumer

| Flag | Description |
|------|--------------|
| `-t, --topic` | Topic to consume from (or regex pattern if `--regex` is used) |
| `-s, --subscription` | Subscription name |
| `--regex` | Treat the topic as a regex pattern to match multiple topics |
| `--subscription-type` | Subscription type: exclusive, shared, or failover |
| `--subscription-initial-position` | Initial position for new subscriptions: earliest or latest (default: latest) |

---

### Producer

| Flag | Description |
|------|--------------|
| `-t, --topic` | Topic to produce to |
| `-p, --property key=value` | Message property (repeatable) |
| `-f, --file` | Read payload from file (sends once per file) |
| `-d, --delimiter` | Custom message delimiter for stdin mode (default: newline) |
| `-c, --enable-chunking` | Enable Pulsar message chunking for large payloads |
| `-b, --enable-batching` | Enable Pulsar message batching |

---

## 🪶 Example Session

```bash
export PULSAR_URL="pulsar://localhost:6650"
export PULSAR_JWT="your-jwt-token"

# Start consumer for a single topic
pulsar-cli consumer -t "demo-topic" -s "demo-sub"

# Start consumer for all topics matching demo-*
pulsar-cli consumer -t "persistent://public/default/demo-.*" -s "demo-sub" --regex

# Produce messages from stdin
echo '{"msg":"hi"}' | pulsar-cli producer -t "demo-topic"

# Produce from file with properties
pulsar-cli producer -t "demo-topic" -f ./payload.json -p app=cli -p format=json
```

---

## 🧾 Logging & Streams

### Separation of streams

- **`stdout` → message payloads** (clean, newline-terminated)
- **`stderr` → logs** (colored, human-readable)

### Piping examples

Ignore logs:

```bash
pulsar-cli reader -t "demo-topic" 2>/dev/null | jq .
```

Split logs and payloads:

```bash
pulsar-cli consumer -t "demo-topic" -s "demo-sub"   1>payloads.log   2>operator.log
```

Inspect logs only:

```bash
pulsar-cli consumer -t "demo-topic" -s "demo-sub" >/dev/null
```

### Sample output

**stderr (logs):**
```
INFO[0000] Consuming from topic pattern persistent://public/default/demo-.* with subscription demo-sub
INFO[0002] received message topic=persistent://public/default/demo-foo msgID=0001 publishAt=2025-11-11T10:00:00Z
```

**stdout (payloads):**
```
{"msg":"hi"}
```

---

## 🧩 Tech Stack

- [Go](https://golang.org/)
- [Apache Pulsar Go Client](https://github.com/apache/pulsar-client-go)
- [Cobra](https://github.com/spf13/cobra)
- [Logrus](https://github.com/sirupsen/logrus)

---

## 🚀 Future Work

- Add schema decoding (JSON/Avro)
- Add topic metadata inspection command
- Add message replay support
- Add version info (build time, commit, tag)
- Add multi-topic reader (simulate regex in reader mode)

---

## 📄 License

MIT License © 2025 Matthias Petermann
