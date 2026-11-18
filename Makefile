SHELL := /bin/sh

DOCKERFILE ?= build/package/Dockerfile

# Mirrors .github/workflows/docker-image.yml build args with local defaults.
APPLICATION ?= $(notdir $(firstword $(wildcard cmd/*)))
BUILD_RFC3339 ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
DESCRIPTION ?= "elasticsearch bulk loader"
PACKAGE ?= $(shell git config --get remote.origin.url 2>/dev/null | sed -e 's/^git@github.com://' -e 's/^https:\/\/github.com\///' -e 's/\.git$$//')
REVISION ?= $(shell git rev-parse HEAD 2>/dev/null || echo local)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo local)

# Image naming is overridable and defaults to PACKAGE:VERSION when PACKAGE is available.
DOCKER_ORGANIZATION ?= $(word 1,$(subst /, ,$(PACKAGE)))
DOCKER_REPOSITORY ?= $(word 2,$(subst /, ,$(PACKAGE)))
IMAGE ?= $(if $(and $(DOCKER_ORGANIZATION),$(DOCKER_REPOSITORY)),$(DOCKER_ORGANIZATION)/$(DOCKER_REPOSITORY):$(VERSION),$(APPLICATION):$(VERSION))

URL ?= http://127.0.0.1:9200

.PHONY: help docker-build print-vars test-unit test-e2e test-e2e-verbose lint

help:
	@echo "Targets:"
	@echo "  make docker-build  Build Docker image using workflow-compatible build args"
	@echo "  make print-vars    Show resolved Docker build variables"
	@echo "  make test-unit     Run unit tests"
	@echo "  make test-e2e      Run canonical Go E2E suite (requires Docker)"
	@echo "  make test-e2e-verbose  Run canonical Go E2E suite with verbose output"
	@echo "  make lint          Run pinned staticcheck SA1019 check, untagged then -tags=e2e (no tests, no Docker)"

print-vars:
	@echo "DOCKERFILE=$(DOCKERFILE)"
	@echo "IMAGE=$(IMAGE)"
	@echo "APPLICATION=$(APPLICATION)"
	@echo "BUILD_RFC3339=$(BUILD_RFC3339)"
	@echo "DESCRIPTION=$(DESCRIPTION)"
	@echo "PACKAGE=$(PACKAGE)"
	@echo "REVISION=$(REVISION)"
	@echo "VERSION=$(VERSION)"

test-unit:
	go test ./...

# -count=1: the E2E suite builds the CLI in a subprocess and talks to a live
# container, so Go's test cache cannot see loader edits or cluster state. A
# cached "ok" here is a false pass.
test-e2e:
	go test -count=1 -tags=e2e ./test -run TestEndToEndScenarios

test-e2e-verbose:
	go test -count=1 -tags=e2e ./test -run TestEndToEndScenarios -v

# Keep SA1019 only: expanding the check set needs separate review.
# Use the go.mod pin to avoid a stale globally installed analyzer.
lint:
	go tool staticcheck -checks=SA1019 ./...
	go tool staticcheck -tags=e2e -checks=SA1019 ./...

docker-build:
	docker buildx build \
		--file $(DOCKERFILE) \
		--build-arg APPLICATION=$(APPLICATION) \
		--build-arg BUILD_RFC3339=$(BUILD_RFC3339) \
		--build-arg DESCRIPTION=$(DESCRIPTION) \
		--build-arg PACKAGE=$(PACKAGE) \
		--build-arg REVISION=$(REVISION) \
		--build-arg VERSION=$(VERSION) \
		--tag $(IMAGE) \
		.

	docker tag $(IMAGE) $(APPLICATION):dev

load-all:
	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index slugs \
		-settings ./examples/slugs/settings.json \
		-mappings ./examples/slugs/mappings.json \
		-pipelines ./examples/slugs/pipelines.json \
		-policies ./examples/slugs/policies.json \
		-data ./examples/slugs/slugs.json \
		-id sanitized \
		-flush \
		-sync-managed \
		-enrich

	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index cards \
		-settings ./examples/cards/settings.json \
		-mappings ./examples/cards/mappings.json \
		-pipelines ./examples/cards/pipelines.json \
		-policies ./examples/slugs/policies.json \
		-data ./examples/cards/cards.json \
		-sync-managed \
		-flush


test-keeplast: es-reset
	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index slugs \
		-settings ./examples/slugs/settings.json \
		-mappings ./examples/slugs/mappings.json \
		-pipelines ./examples/slugs/pipelines.json \
		-policies ./examples/slugs/policies.json \
		-data ./examples/slugs/slugs.json \
		-id sanitized \
		-flush \
		-sync-managed \
		-enrich

	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index slugs \
		-settings ./examples/slugs/settings.json \
		-mappings ./examples/slugs/mappings.json \
		-pipelines ./examples/slugs/pipelines.json \
		-policies ./examples/slugs/policies.json \
		-data ./examples/slugs/slugs.json \
		-id sanitized \
		-flush \
		-sync-managed \
		-enrich

	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index slugs \
		-settings ./examples/slugs/settings.json \
		-mappings ./examples/slugs/mappings.json \
		-pipelines ./examples/slugs/pipelines.json \
		-policies ./examples/slugs/policies.json \
		-data ./examples/slugs/slugs.json \
		-id sanitized \
		-flush \
		-sync-managed \
		-enrich

	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index cards \
		-settings ./examples/cards/settings.json \
		-mappings ./examples/cards/mappings.json \
		-pipelines ./examples/cards/pipelines.json \
		-data ./examples/cards/cards.json \
		-sync-managed \
		-delete \
		-alias \
		-keep-last 2

	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index cards \
		-settings ./examples/cards/settings.json \
		-mappings ./examples/cards/mappings.json \
		-pipelines ./examples/cards/pipelines.json \
		-data ./examples/cards/cards.json \
		-sync-managed \
		-delete \
		-alias \
		-keep-last 2

	go run ./cmd/es-bulk-loader \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index cards \
		-settings ./examples/cards/settings.json \
		-mappings ./examples/cards/mappings.json \
		-pipelines ./examples/cards/pipelines.json \
		-data ./examples/cards/cards.json \
		-sync-managed \
		-delete \
		-alias \
		-keep-last 2

example:
	docker run --rm \
		-v ./examples/slugs:/data:ro es-bulk-loader:dev \
		-url ${URL} \
		-insecureSkipVerify=true \
		-index slugs \
		-settings /data/settings.json \
		-mappings /data/mappings.json \
		-pipelines /data/pipelines.json \
		-policies /data/policies.json \
		-data /data/slugs.json \
		-id sanitized \
		-flush \
		-sync-managed \
		-enrich

testing:
	docker run --rm -v \
	./test/fixtures:/data:ro es-bulk-loader:dev \
	-url ${URL} \
	-insecureSkipVerify=true \
	-index index1 \
	-settings /data/index1-settings.json \
	-mappings /data/index1-mappings.json \
	-pipelines /data/index1-pipelines.json \
	-policies /data/index1-policies.json \
	-data /data/index1-data.json \
	-id lookup_id \
	-delete \
	-sync-managed \
	-enrich


# ----
# --- Elasticsearch (single-node, no security) ---
ES_VERSION ?= 9.3.1
ES_PORT    ?= 9200

ES_CONTAINER := es-dev

.PHONY: es-up es-down es-logs es-reset es-wait

es-up:
	@docker rm -f $(ES_CONTAINER) >/dev/null 2>&1 || true
	@docker run -d --name $(ES_CONTAINER) \
		-e discovery.type=single-node \
		-e xpack.security.enabled=false \
		-e xpack.license.self_generated.type=basic \
		-e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
		-p $(ES_PORT):9200 \
		$(if $(shell docker version -f '{{.Server.Os}}' 2>/dev/null | grep -qi linux && echo yes),--add-host=host.docker.internal:host-gateway,) \
		docker.elastic.co/elasticsearch/elasticsearch:$(ES_VERSION)
	@$(MAKE) es-wait

es-wait:
	@printf "Waiting for Elasticsearch on http://127.0.0.1:$(ES_PORT) "
	@i=0; \
	while [ $$i -lt 60 ]; do \
		if curl -fsS "http://127.0.0.1:$(ES_PORT)/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null 2>&1; then \
			echo "OK"; exit 0; \
		fi; \
		printf "."; \
		i=$$((i+1)); \
		sleep 1; \
	done; \
	echo "\nTimed out waiting for Elasticsearch"; \
	docker logs $(ES_CONTAINER) --tail 200; \
	exit 1

es-logs:
	@docker logs -f $(ES_CONTAINER)

es-down:
	@docker rm -f $(ES_CONTAINER) >/dev/null 2>&1 || true

es-reset: es-down es-up
