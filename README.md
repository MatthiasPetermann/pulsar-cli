# Pulsar CLI

A lightweight **command-line client for Apache Pulsar** written in Go.  
It supports producing, reading, and consuming messages from Pulsar topics — with **colored logs on `stderr`** and **message payloads on `stdout`** for easy piping.

---

## ✨ Features

- **Reader** — read messages from a given topic without a subscription  
- **Consumer** — consume messages with a subscription  
- **Producer** — publish messages (read from `stdin`) to a topic  
- **Colored logging** with `logrus` to **`stderr`** (Unix-style)  
- **Payloads to `stdout`** → pipe-friendly for tooling (`jq`, `grep`, etc.)  
- **Environment-based configuration** for Pulsar connection  
- **Static, portable builds** (no external dependencies)

---

## 📦 Download & Installation

### 🧰 Build from Source

Clone and build it yourself:

```bash
git clone https://forge.ext.d2ux.net/Atlas/pulsar-cli
cd pulsar-cli

go mod tidy
make build
```

Resulting binary will be at `./pulsar-cli`.

For a static cross-platform build:

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

Read messages from a topic (no subscription).

```bash
pulsar-cli reader -t "my-topic"
```

### Consumer

Consume messages with a subscription.

```bash
pulsar-cli consumer -t "my-topic" -s "my-subscription"
```

### Producer

Publish messages (from `stdin`) to a topic.

```bash
echo "Hello, Pulsar!" | pulsar-cli producer -t "my-topic"
```

---

## 🪶 Example

```bash
export PULSAR_URL="pulsar://localhost:6650"
export PULSAR_JWT="your-jwt-token"

# Start consumer (stderr shows colored logs, stdout prints payloads)
pulsar-cli consumer -t "demo-topic" -s "demo-sub"

# Send a test message
echo '{"msg":"hi"}' | pulsar-cli producer -t "demo-topic"
```

---

## 🧾 Logging & Streams

**Separation of concerns:**

- **`stdout` → message payloads** (exact bytes, newline-terminated)
- **`stderr` → operational logs** (colored, human-friendly; timestamps, metadata)

### Practical examples

Pipe only the **payloads** (ignore logs):

```bash
pulsar-cli reader -t "demo-topic" 2>/dev/null | jq .
```

Capture logs and payloads separately:

```bash
pulsar-cli consumer -t "demo-topic" -s "demo-sub"   1>payloads.log   2>operator.log
```

Inspect only the logs (human-readable):

```bash
pulsar-cli consumer -t "demo-topic" -s "demo-sub" >/dev/null
```

### Sample output

**stderr (logs, colored in a TTY):**
```
INFO[0000] Reading from topic demo-topic ...
INFO[0001] received message topic=demo-topic msgID=AQAAAHt... publishAt=2025-10-15T10:12:31Z
```

**stdout (payloads):**
```
{"msg":"hi"}
```

> Note: Previous versions logged the payload inside JSON logs.  
> Now the payload is printed to **stdout** instead, which makes piping reliable.

---

## 🧩 Tech Stack

- [Go](https://golang.org/)
- [Apache Pulsar Go Client](https://github.com/apache/pulsar-client-go)
- [Cobra](https://github.com/spf13/cobra)
- [Logrus](https://github.com/sirupsen/logrus)

---

## 📄 License

MIT License © 2025 Matthias Petermann

---

## 🚀 Future Work

- Add message schema decoding (JSON/Avro)
- Add topic metadata inspection command
- Add version info (build time, commit, tag)
