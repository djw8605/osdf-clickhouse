# Makefile for the osdf-clickhouse pipeline.
# The Go module lives in ./ingester.

INGESTER_DIR := ingester
IMAGE        ?= ghcr.io/djw8605/osdf-clickhouse-ingester
TAG          ?= 0.1.0

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the ingester binary
	cd $(INGESTER_DIR) && CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/ingester ./cmd/ingester

.PHONY: test
test: ## Run unit tests
	cd $(INGESTER_DIR) && go test ./...

.PHONY: test-integration
test-integration: ## Run integration tests (needs Docker; build tag "integration")
	cd $(INGESTER_DIR) && go test -tags=integration -timeout=10m ./test/...

.PHONY: vet
vet: ## go vet
	cd $(INGESTER_DIR) && go vet ./...

.PHONY: fmt
fmt: ## gofmt -w
	cd $(INGESTER_DIR) && gofmt -w .

.PHONY: lint
lint: ## Run golangci-lint if installed, else go vet
	@if command -v golangci-lint >/dev/null 2>&1; then \
		cd $(INGESTER_DIR) && golangci-lint run ./...; \
	else \
		echo "golangci-lint not found; falling back to go vet"; \
		$(MAKE) vet; \
	fi

.PHONY: tidy
tidy: ## go mod tidy
	cd $(INGESTER_DIR) && go mod tidy

.PHONY: docker
docker: ## Build the container image
	docker build -t $(IMAGE):$(TAG) $(INGESTER_DIR)

.PHONY: docker-push
docker-push: ## Push the container image
	docker push $(IMAGE):$(TAG)

.PHONY: yaml-validate
yaml-validate: ## Parse-check all Kubernetes YAML
	@python3 -c "import yaml,glob,sys; \
	[list(yaml.safe_load_all(open(f))) for f in glob.glob('clickhouse/**/*.yaml',recursive=True)+glob.glob('deploy/**/*.yaml',recursive=True)]; \
	print('all YAML OK')"

.PHONY: ci
ci: vet test yaml-validate ## Fast checks for CI (no Docker)
