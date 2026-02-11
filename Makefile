# =========================================================
# 🌀 Pulsar CLI - Makefile (fixed)
# =========================================================

# Name des Produkts / Binaries
PRODUCT := pulsar-cli

# Versionstag (aus Git oder Fallback)
BUILD_TAG := $(shell git describe --tags --always 2>/dev/null || echo "dev")
TAG := $(shell echo $(BUILD_TAG) | sed 's/^v//')

# Plattforminformationen
ARCH := $(shell go env GOARCH)
OS   := $(shell go env GOOS)

# Go-Build-Parameter
GO        := go
GOFLAGS   := -trimpath
LDFLAGS   := -s -w -X main.Version=$(BUILD_TAG)
BUILDFLAGS := CGO_ENABLED=0

# =========================================================
# 🧱 Build Targets
# =========================================================

all: package

# --- Lokaler Build ------------------------------------------------
build:
	@echo "🔨 Building $(PRODUCT) ($(BUILD_TAG)) for $(OS)/$(ARCH)..."
	$(BUILDFLAGS) $(GO) build $(GOFLAGS) -ldflags "-extldflags '-static' $(LDFLAGS)" -o $(PRODUCT) .

package: build
	@echo "📦 Packaging $(PRODUCT) $(BUILD_TAG)..."
	mkdir -p dist/$(PRODUCT)-$(TAG)-$(OS)-$(ARCH)
	cp $(PRODUCT) dist/$(PRODUCT)-$(TAG)-$(OS)-$(ARCH)/
	@if [ -f README.md ]; then cp README.md dist/$(PRODUCT)-$(TAG)-$(OS)-$(ARCH)/; fi
	tar -czvf $(PRODUCT)-$(TAG)-$(OS)-$(ARCH).tar.gz -C dist $(PRODUCT)-$(TAG)-$(OS)-$(ARCH)

# --- Linux --------------------------------------------------------
build-linux:
	@echo "🐧 Building static Linux binary..."
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "-extldflags '-static' $(LDFLAGS)" -o $(PRODUCT) .

package-linux: build-linux
	@echo "📦 Packaging for Linux..."
	mkdir -p dist/$(PRODUCT)-$(TAG)-linux-amd64
	cp $(PRODUCT) dist/$(PRODUCT)-$(TAG)-linux-amd64/
	@if [ -f README.md ]; then cp README.md dist/$(PRODUCT)-$(TAG)-linux-amd64/; fi
	tar -czvf $(PRODUCT)-$(TAG)-linux-amd64.tar.gz -C dist $(PRODUCT)-$(TAG)-linux-amd64
	rm -f $(PRODUCT)

# --- macOS --------------------------------------------------------
build-macos:
	@echo "🍎 Building static macOS binary..."
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "-extldflags '-static' $(LDFLAGS)" -o $(PRODUCT) .

package-macos: build-macos
	@echo "📦 Packaging for macOS..."
	mkdir -p dist/$(PRODUCT)-$(TAG)-macos-arm64
	cp $(PRODUCT) dist/$(PRODUCT)-$(TAG)-macos-arm64/
	@if [ -f README.md ]; then cp README.md dist/$(PRODUCT)-$(TAG)-macos-arm64/; fi
	tar -czvf $(PRODUCT)-$(TAG)-macos-arm64.tar.gz -C dist $(PRODUCT)-$(TAG)-macos-arm64
	rm -f $(PRODUCT)

# --- Windows ------------------------------------------------------
build-windows:
	@echo "🪟 Building static Windows binary..."
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "-extldflags '-static' $(LDFLAGS)" -o $(PRODUCT).exe .

package-windows: build-windows
	@echo "📦 Packaging for Windows..."
	mkdir -p dist/$(PRODUCT)-$(TAG)-windows-amd64
	cp $(PRODUCT).exe dist/$(PRODUCT)-$(TAG)-windows-amd64/
	@if [ -f README.md ]; then cp README.md dist/$(PRODUCT)-$(TAG)-windows-amd64/; fi
	tar -czvf $(PRODUCT)-$(TAG)-windows-amd64.tar.gz -C dist $(PRODUCT)-$(TAG)-windows-amd64
	rm -f $(PRODUCT).exe

# --- Clean --------------------------------------------------------
clean:
	@echo "🧹 Cleaning..."
	rm -rf dist $(PRODUCT) $(PRODUCT).exe *.tar.gz

# --- Convenience Targets -----------------------------------------
release: package-linux package-macos package-windows

.DEFAULT_GOAL := all
.PHONY: all build package build-linux package-linux build-macos package-macos build-windows package-windows release clean
