# Name des Binaries
BINARY_NAME = pulsar-cli

# Go-Build-Parameter
GO      = go
GOFLAGS = -trimpath
LDFLAGS = -s -w
BUILDFLAGS = CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH)

# Default-Ziel (baut lokal für deine aktuelle Plattform)
all: build

# Statischer Build für die aktuelle Plattform
build:
	@echo "🔨 Building $(BINARY_NAME)..."
	$(BUILDFLAGS) $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) ./...

# Statischer Cross-Build für Linux (amd64)
build-linux:
	@echo "🐧 Building static Linux binary..."
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)-linux ./...

# Statischer Cross-Build für macOS (arm64)
build-macos:
	@echo "🍎 Building static macOS binary..."
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_NAME)-macos ./...

# Cross-Build für Windows
build-windows:
	@echo "🪟 Building static Windows binary..."
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY_NAME).exe ./...

# Testet Code und prüft auf Fehler
test:
	@echo "🧪 Running tests..."
	$(GO) test ./...

# Entfernt gebaute Artefakte
clean:
	@echo "🧹 Cleaning..."
	rm -f $(BINARY_NAME) $(BINARY_NAME)-linux $(BINARY_NAME)-macos $(BINARY_NAME).exe

.PHONY: all build build-linux build-macos build-windows test clean

