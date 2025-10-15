# Pulsar CLI

A lightweight **command-line client for Apache Pulsar** written in Go.  
It supports producing, reading, and consuming messages from Pulsar topics — using structured logging via **logrus** and flexible command-line flags powered by **cobra**.

---

## ✨ Features

- **Reader** — read messages from a given topic without a subscription  
- **Consumer** — consume messages with a subscription  
- **Producer** — publish messages (read from `stdin`) to a topic  
- **Structured JSON logging** with `logrus`  
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

| Variable       | Description                            | Example                              |
|----------------|----------------------------------------|--------------------------------------|
| `PULSAR_URL`   | Pulsar broker URL                      | `pulsar://localhost:6650`            |
| `PULSAR_JWT`   | (Optional) JWT token for authentication | `eyJhbGciOiJIUzI1NiIsInR5cCI6...`   |

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

# Start consumer
pulsar-cli consumer -t "demo-topic" -s "demo-sub"

# Send a test message
echo '{"msg":"hi"}' | pulsar-cli producer -t "demo-topic"
```

---

## 🧾 Logging Example

Output (JSON formatted via `logrus`):

```json
{
  "level": "info",
  "msg": "received message",
  "topic": "demo-topic",
  "msgID": "AQAAAHt...",
  "payload": "{\"msg\":\"hi\"}",
  "publishAt": "2025-10-15T10:12:31Z"
}
```

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

