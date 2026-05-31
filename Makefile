SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

ROOT := $(shell pwd)
KUBECTL := kubectl
HELM := helm

NS_HATCH := hatch
NS_OBS := observability

# HOST_DATABASE_URL is the localhost-via-port-forward DSN. Cluster services
# use DATABASE_URL (ClusterDNS) from the hatch-secrets Secret. Never overlap.
HOST_DATABASE_URL ?= postgres://hatch:hatchpass@localhost:5432/hatch?sslmode=disable

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: build-api build-scheduler build-delivery-worker build-retry-consumer build-reconciliation-cron build-partition-archival ## Build all Hatch service Docker images

.PHONY: build-api
build-api: swag-gen ## Build the scheduler-api Docker image with a unique tag
	@TAG=dev-$$(date +%s); \
	  docker build -f Dockerfile.api -t hatch/api:$$TAG -t hatch/api:dev . && \
	  echo $$TAG > .api-image-tag && \
	  echo "→ tagged: hatch/api:$$TAG (also hatch/api:dev)"

.PHONY: build-scheduler
build-scheduler: ## Build the scheduler-service Docker image with a unique tag
	@TAG=dev-$$(date +%s); \
	  docker build -f Dockerfile.scheduler -t hatch/scheduler:$$TAG -t hatch/scheduler:dev . && \
	  echo $$TAG > .scheduler-image-tag && \
	  echo "→ tagged: hatch/scheduler:$$TAG (also hatch/scheduler:dev)"

.PHONY: build-delivery-worker
build-delivery-worker: ## Build the delivery-worker Docker image with a unique tag
	@TAG=dev-$$(date +%s); \
	  docker build -f Dockerfile.delivery-worker -t hatch/delivery-worker:$$TAG -t hatch/delivery-worker:dev . && \
	  echo $$TAG > .delivery-worker-image-tag && \
	  echo "→ tagged: hatch/delivery-worker:$$TAG (also hatch/delivery-worker:dev)"

.PHONY: build-retry-consumer
build-retry-consumer: ## Build the retry-consumer Docker image with a unique tag
	@TAG=dev-$$(date +%s); \
	  docker build -f Dockerfile.retry-consumer -t hatch/retry-consumer:$$TAG -t hatch/retry-consumer:dev . && \
	  echo $$TAG > .retry-consumer-image-tag && \
	  echo "→ tagged: hatch/retry-consumer:$$TAG (also hatch/retry-consumer:dev)"

.PHONY: build-reconciliation-cron
build-reconciliation-cron: ## Build the reconciliation-cron Docker image with a unique tag
	@TAG=dev-$$(date +%s); \
	  docker build -f Dockerfile.reconciliation-cron -t hatch/reconciliation-cron:$$TAG -t hatch/reconciliation-cron:dev . && \
	  echo $$TAG > .reconciliation-cron-image-tag && \
	  echo "→ tagged: hatch/reconciliation-cron:$$TAG (also hatch/reconciliation-cron:dev)"

.PHONY: build-partition-archival
build-partition-archival: ## Build the partition-archival Docker image with a unique tag
	@TAG=dev-$$(date +%s); \
	  docker build -f Dockerfile.partition-archival -t hatch/partition-archival:$$TAG -t hatch/partition-archival:dev . && \
	  echo $$TAG > .partition-archival-image-tag && \
	  echo "→ tagged: hatch/partition-archival:$$TAG (also hatch/partition-archival:dev)"

.PHONY: build-verify
build-verify: ## Build the in-cluster verify Docker image with a unique tag
	@TAG=dev-$$(date +%s); \
	  docker build -f Dockerfile.verify -t hatch/verify:$$TAG -t hatch/verify:dev . && \
	  echo $$TAG > .verify-image-tag && \
	  echo "→ tagged: hatch/verify:$$TAG (also hatch/verify:dev)"

.PHONY: swag-gen
swag-gen: ## Regenerate OpenAPI spec under docs/ from handler annotations
	go tool swag init \
	  -g cmd/api/main.go \
	  -o docs \
	  --parseInternal \
	  --parseDependency

.PHONY: run-api
run-api: ## Run scheduler-api locally against HOST_* DSNs (no k8s)
	@set -a; . ./.env; set +a; \
	  DATABASE_URL="$$HOST_DATABASE_URL" REDIS_ADDR="$$HOST_REDIS_ADDR" \
	  OTLP_ENDPOINT="" \
	  go run ./cmd/api

.PHONY: run-scheduler
run-scheduler: ## Run scheduler-service locally against HOST_* DSNs (single pod)
	@set -a; . ./.env; set +a; \
	  DATABASE_URL="$$HOST_DATABASE_URL" \
	  KAFKA_BROKERS="$$HOST_KAFKA_BROKERS" \
	  POD_INDEX=0 TOTAL_PODS=1 \
	  SCHEDULER_WHEEL_DB_PATH="$${SCHEDULER_WHEEL_DB_PATH:-./.local-wheel.db}" \
	  OTLP_ENDPOINT="" \
	  go run ./cmd/scheduler

.PHONY: run-delivery-worker
run-delivery-worker: ## Run delivery-worker locally against HOST_* DSNs (no k8s)
	@set -a; . ./.env; set +a; \
	  DATABASE_URL="$$HOST_DATABASE_URL" \
	  KAFKA_BROKERS="$$HOST_KAFKA_BROKERS" \
	  REDIS_ADDR="$$HOST_REDIS_ADDR" \
	  OTLP_ENDPOINT="" \
	  go run ./cmd/delivery-worker

.PHONY: run-retry-consumer
run-retry-consumer: ## Run retry-consumer locally against HOST_* brokers (no k8s)
	@set -a; . ./.env; set +a; \
	  KAFKA_BROKERS="$$HOST_KAFKA_BROKERS" \
	  OTLP_ENDPOINT="" \
	  go run ./cmd/retry-consumer

.PHONY: run-reconciliation-cron
run-reconciliation-cron: ## Run reconciliation-cron locally against HOST_* DSNs (no k8s)
	@set -a; . ./.env; set +a; \
	  DATABASE_URL="$$HOST_DATABASE_URL" \
	  KAFKA_BROKERS="$$HOST_KAFKA_BROKERS" \
	  OTLP_ENDPOINT="" \
	  go run ./cmd/reconciliation-cron

.PHONY: run-partition-archival
run-partition-archival: ## Run partition-archival locally against HOST_* DSNs (no k8s)
	@set -a; . ./.env; set +a; \
	  DATABASE_URL="$$HOST_DATABASE_URL" \
	  ARCHIVE_DIR="$${ARCHIVE_DIR:-./.local-archive}" \
	  OTLP_ENDPOINT="" \
	  go run ./cmd/partition-archival

.PHONY: gen-provider-key
gen-provider-key: ## Print a base64 Tink AES256-GCM keyset for PROVIDER_CRED_KEY
	@go run ./cmd/tinkgen

.PHONY: deps
deps: ## Pull helm chart dependencies
	cd helm/observability && $(HELM) dependency update

.PHONY: up-obs-crds
up-obs-crds: deps ## Refresh Prometheus Operator CRDs for observability
	@./scripts/apply-observability-crds.sh

.PHONY: up
up: ## Deploy hatch in three phases: infra, jobs, then service pods. Assumes obs is already up.
	$(MAKE) up-infra
	$(MAKE) up-jobs
	$(MAKE) up-pods

.PHONY: up-infra
up-infra: ## Bring up hatch infra only (postgres/redis/kafka)
	@./scripts/inject-secrets.sh
	@./scripts/sync-migrations.sh
	@echo "→ deploying hatch infra (postgres, redis, kafka)"; \
	  $(HELM) upgrade --install hatch ./helm/hatch \
	    --namespace $(NS_HATCH) --create-namespace \
	    --set api.enabled=false \
	    --set scheduler.enabled=false \
	    --set deliveryWorker.enabled=false \
	    --set retryConsumer.enabled=false \
	    --set reconciliationCron.enabled=false \
	    --set partitionArchival.enabled=false \
	    --set migrations.enabled=false \
	    --set kafka.topicsJob.enabled=false \
	    --wait --wait-for-jobs --timeout 5m
	@echo
	@echo "Hatch infra is up. Next: make up-jobs"

.PHONY: up-jobs
up-jobs: ## Bring up hatch jobs only (migrations + topic bootstrap)
	@./scripts/inject-secrets.sh
	@./scripts/sync-migrations.sh
	@echo "→ deploying hatch jobs (db migrations, kafka topics)"; \
	  $(HELM) upgrade --install hatch ./helm/hatch \
	    --namespace $(NS_HATCH) --create-namespace \
	    --set api.enabled=false \
	    --set scheduler.enabled=false \
	    --set deliveryWorker.enabled=false \
	    --set retryConsumer.enabled=false \
	    --set reconciliationCron.enabled=false \
	    --set partitionArchival.enabled=false \
	    --set migrations.enabled=true \
	    --set kafka.topicsJob.enabled=true \
	    --wait --wait-for-jobs --timeout 5m
	@echo
	@echo "Hatch jobs are up. Next: make up-pods"

.PHONY: up-pods
up-pods: ## Bring up hatch service pods only (api, scheduler, workers, crons)
	@API_TAG=$$([ -f .api-image-tag ] && cat .api-image-tag || echo dev); \
	 SCHED_TAG=$$([ -f .scheduler-image-tag ] && cat .scheduler-image-tag || echo dev); \
	 DW_TAG=$$([ -f .delivery-worker-image-tag ] && cat .delivery-worker-image-tag || echo dev); \
	 RC_TAG=$$([ -f .retry-consumer-image-tag ] && cat .retry-consumer-image-tag || echo dev); \
	 RECON_TAG=$$([ -f .reconciliation-cron-image-tag ] && cat .reconciliation-cron-image-tag || echo dev); \
	 ARCH_TAG=$$([ -f .partition-archival-image-tag ] && cat .partition-archival-image-tag || echo dev); \
	  echo "→ deploying api with hatch/api:$$API_TAG, scheduler with hatch/scheduler:$$SCHED_TAG, delivery-worker with hatch/delivery-worker:$$DW_TAG, retry-consumer with hatch/retry-consumer:$$RC_TAG, reconciliation-cron with hatch/reconciliation-cron:$$RECON_TAG, partition-archival with hatch/partition-archival:$$ARCH_TAG"; \
	  $(HELM) upgrade --install hatch ./helm/hatch \
	    --namespace $(NS_HATCH) --create-namespace \
	    --set api.enabled=true \
	    --set api.image=hatch/api:$$API_TAG \
	    --set scheduler.enabled=true \
	    --set scheduler.image=hatch/scheduler:$$SCHED_TAG \
	    --set deliveryWorker.enabled=true \
	    --set deliveryWorker.image=hatch/delivery-worker:$$DW_TAG \
	    --set retryConsumer.enabled=true \
	    --set retryConsumer.image=hatch/retry-consumer:$$RC_TAG \
	    --set reconciliationCron.enabled=true \
	    --set reconciliationCron.image=hatch/reconciliation-cron:$$RECON_TAG \
	    --set partitionArchival.enabled=true \
	    --set partitionArchival.image=hatch/partition-archival:$$ARCH_TAG \
	    --set migrations.enabled=false \
	    --set kafka.topicsJob.enabled=false \
	    --wait --wait-for-jobs --timeout 5m
	@echo
	@echo "Hatch up. Port-forward with: make port-forward"

.PHONY: down
down: pf-stop ## Tear down hatch helm release (keeps PVCs, leaves obs running)
	-$(HELM) uninstall hatch -n $(NS_HATCH)

.PHONY: restart
restart: ## Restart hatch only (down + up, keeps PVCs and obs)
	$(MAKE) down
	$(MAKE) up

.PHONY: up-obs
up-obs: ## Deploy observability stack (grafana/prom/loki/tempo)
	$(MAKE) up-obs-crds
	$(HELM) upgrade --install observability ./helm/observability \
	  --namespace $(NS_OBS) --create-namespace \
	  --skip-crds \
	  --set kps.crds.enabled=false \
	  --wait --timeout 10m

.PHONY: down-obs
down-obs: ## Tear down observability helm release (keeps PVCs)
	-$(HELM) uninstall observability -n $(NS_OBS)

.PHONY: up-all
up-all: ## First-time setup: deploy obs then hatch
	$(MAKE) up-obs
	$(MAKE) up

.PHONY: down-all
down-all: down down-obs ## Tear down both helm releases (keeps PVCs)

.PHONY: reset
reset: ## Nuclear option: tear down everything, wipe PVCs, redeploy clean
	$(MAKE) down-all
	-$(KUBECTL) -n $(NS_HATCH) delete pvc --all
	-$(KUBECTL) -n $(NS_OBS) delete pvc --all
	$(MAKE) up-all

.PHONY: port-forward
port-forward: ## Start port-forwards in the background
	@./scripts/port-forward.sh

.PHONY: pf-stop
pf-stop: ## Stop any running kubectl port-forward processes
	-pkill -f "kubectl port-forward" || true

.PHONY: status
status: ## Show pod status across both namespaces
	@$(KUBECTL) get pods -n $(NS_HATCH) -o wide
	@echo "---"
	@$(KUBECTL) get pods -n $(NS_OBS) -o wide

.PHONY: logs
logs: ## Tail logs for SVC=<component> (e.g. SVC=postgres)
	@test -n "$(SVC)" || (echo "SVC required, e.g. make logs SVC=postgres" && exit 1)
	$(KUBECTL) logs -f -l app.kubernetes.io/component=$(SVC) -n $(NS_HATCH) --tail=200

.PHONY: migrate
migrate: ## Run golang-migrate up against local Postgres
	migrate -path migrations -database "$(HOST_DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Roll back all migrations
	migrate -path migrations -database "$(HOST_DATABASE_URL)" down -all

.PHONY: sqlc
sqlc: ## Regenerate Go from queries via sqlc
	sqlc generate

.PHONY: test
test: ## Run all unit tests under -race
	go test -race ./pkg/... ./internal/...

.PHONY: verify
verify: ## Run the full cumulative acceptance audit (host prelude + in-cluster Job)
	@./scripts/verify.sh
