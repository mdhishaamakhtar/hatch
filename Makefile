SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# The commands in cmd/ that run in the cluster.
SERVICES := api scheduler delivery-worker retry-consumer reconciliation-cron partition-archival

# Postgres as `make port-forward` exposes it.
HOST_DATABASE_URL ?= postgres://hatch:hatchpass@localhost:5432/hatch?sslmode=disable

HELM_HATCH := helm upgrade --install hatch ./helm/hatch --namespace hatch --create-namespace --wait --wait-for-jobs --timeout 5m

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_%-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ─── Develop ────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run the tests
	go test -race ./...

.PHONY: sqlc
sqlc: ## Regenerate internal/db from queries/ and migrations/
	sqlc generate

.PHONY: swag
swag: ## Regenerate the OpenAPI spec in docs/ from the API's annotations
	go tool swag init -g cmd/api/main.go -o docs --parseInternal --parseDependency --useStructName

.PHONY: gen-provider-key
gen-provider-key: ## Print a new keyset for PROVIDER_CRED_KEY
	@go run ./cmd/tinkgen

run-%: ## Run a service against the port-forwarded stack, e.g. make run-api
	@set -a; . ./.env; set +a; mkdir -p .local; \
	  DATABASE_URL="$$HOST_DATABASE_URL" REDIS_ADDR="$$HOST_REDIS_ADDR" KAFKA_BROKERS="$$HOST_KAFKA_BROKERS" \
	  OTLP_ENDPOINT= POD_INDEX=0 TOTAL_PODS=1 SCHEDULER_WHEEL_DB_PATH=.local/wheel.db ARCHIVE_DIR=.local/archive \
	  go run ./cmd/$*

# ─── Images ─────────────────────────────────────────────────────────────────

.PHONY: build
build: $(addprefix build-,$(SERVICES)) ## Build every service's image

# Each build gets a tag of its own, which is what makes Kubernetes roll the
# pods over to it; `make up-pods` deploys the tag last built.
build-api: swag
build-%: ## Build one command's image, e.g. make build-api
	@tag=dev-$$(date +%s); \
	  docker build --build-arg CMD=$* -t hatch/$*:$$tag -t hatch/$*:dev . && \
	  echo $$tag > .$*-image-tag && \
	  echo "→ hatch/$*:$$tag"

# ─── Deploy ─────────────────────────────────────────────────────────────────

.PHONY: up
up: ## Deploy Hatch: the infrastructure, then the migrations and topics, then the services
	@./scripts/inject-secrets.sh
	@./scripts/sync-migrations.sh
	$(HELM_HATCH) --set services.enabled=false --set jobs.enabled=false
	$(HELM_HATCH) --set services.enabled=false --set jobs.enabled=true
	$(MAKE) up-pods

.PHONY: up-pods
up-pods: ## Deploy the services, with the images `make build` last built
	@images=""; \
	  for s in $(SERVICES); do images="$$images --set images.$$s=hatch/$$s:$$(cat .$$s-image-tag 2>/dev/null || echo dev)"; done; \
	  $(HELM_HATCH) --set services.enabled=true --set jobs.enabled=false $$images

.PHONY: down
down: ## Uninstall Hatch, keeping its volumes
	-pkill -f "kubectl port-forward"
	-helm uninstall hatch -n hatch

.PHONY: up-obs
up-obs: ## Deploy Prometheus, Grafana, Loki and Tempo
	cd helm/observability && helm dependency update
	@./scripts/apply-observability-crds.sh
	helm upgrade --install observability ./helm/observability --namespace observability --create-namespace \
	  --skip-crds --set kps.crds.enabled=false --wait --timeout 10m

.PHONY: down-obs
down-obs: ## Uninstall the observability stack, keeping its volumes
	-helm uninstall observability -n observability

.PHONY: up-all
up-all: up-obs up ## Deploy the observability stack, then Hatch

.PHONY: down-all
down-all: down down-obs ## Uninstall everything, keeping the volumes

.PHONY: reset
reset: down-all ## Uninstall everything, delete the volumes, and deploy again
	-kubectl -n hatch delete pvc --all
	-kubectl -n observability delete pvc --all
	$(MAKE) up-all

.PHONY: port-forward
port-forward: ## Forward Postgres, Redis and Kafka to localhost
	@./scripts/port-forward.sh

.PHONY: migrate
migrate: ## Apply the migrations to the port-forwarded database
	migrate -path migrations -database "$(HOST_DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Roll back every migration on the port-forwarded database
	migrate -path migrations -database "$(HOST_DATABASE_URL)" down -all

.PHONY: status
status: ## Show the pods
	@kubectl get pods -n hatch -o wide
	@kubectl get pods -n observability -o wide

.PHONY: logs
logs: ## Follow a component's logs, e.g. make logs SVC=scheduler
	@test -n "$(SVC)" || { echo "usage: make logs SVC=<component>"; exit 1; }
	kubectl logs -f -l app.kubernetes.io/component=$(SVC) -n hatch --tail=200 --max-log-requests=10

# ─── Verify and benchmark ───────────────────────────────────────────────────

.PHONY: verify
verify: ## Check the deployed stack end to end
	@./scripts/verify.sh

.PHONY: bench
bench: ## Run one benchmark, e.g. make bench SCENARIO=delivery COUNT=2000
	@./scripts/bench.sh one $${SCENARIO:-e2e} $${COUNT:-400} $${WORKERS:-32} $${SPREAD:-0s} "$${LABEL:-manual}"

.PHONY: bench-all
bench-all: ## Run the reference benchmarks and write benchmarks/ (about an hour)
	@./scripts/bench.sh all
