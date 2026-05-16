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
build: ## Build all service Docker images (Phase 0 stub)
	@echo "Phase 0: no service images yet."

.PHONY: deps
deps: ## Pull helm chart dependencies
	cd helm/observability && $(HELM) dependency update

.PHONY: up
up: deps ## Inject secrets, deploy observability + hatch (data infra only)
	@./scripts/inject-secrets.sh
	$(HELM) upgrade --install observability ./helm/observability \
	  --namespace $(NS_OBS) --create-namespace \
	  --wait --timeout 10m
	$(HELM) upgrade --install hatch ./helm/hatch \
	  --namespace $(NS_HATCH) --create-namespace \
	  --wait --timeout 5m
	@echo
	@echo "Stack up. Port-forward with: make port-forward"

.PHONY: port-forward
port-forward: ## Start port-forwards in the background
	@./scripts/port-forward.sh

.PHONY: pf-stop
pf-stop: ## Stop any running kubectl port-forward processes
	-pkill -f "kubectl port-forward" || true

.PHONY: down
down: pf-stop ## Tear down helm releases (keeps PVCs)
	-$(HELM) uninstall hatch -n $(NS_HATCH)
	-$(HELM) uninstall observability -n $(NS_OBS)

.PHONY: restart
restart: ## Tear down, wipe PVCs, bring stack back up clean
	$(MAKE) down
	-$(KUBECTL) -n $(NS_HATCH) delete pvc --all
	-$(KUBECTL) -n $(NS_OBS) delete pvc --all
	$(MAKE) up

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
test: ## go test ./pkg/...
	go test ./pkg/...

.PHONY: phase0-verify
phase0-verify: ## Run every Phase 0 acceptance check and report
	@./scripts/phase0-verify.sh
