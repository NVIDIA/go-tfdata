SHELL := /bin/bash
SCRIPTS_DIR = ./deploy

# Colors
cyan = $(shell { tput setaf 6 || tput AF 6; } 2>/dev/null)
term-reset = $(shell { tput sgr0 || tput me; } 2>/dev/null)
$(call make-lazy,cyan)
$(call make-lazy,term-reset)

#
# go modules
#

.PHONY: mod mod-clean mod-tidy

mod: mod-clean mod-tidy

# cleanup gomod cache
mod-clean: ## Clean modules
	go clean --modcache

mod-tidy: ## Remove unused modules from 'go.mod'
	go mod tidy

#
# tests
#
.PHONY: test

test:
	$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test

ci: spell-check fmt-check lint test ## Run CI related checkers and linters (requires BUCKET variable to be set)

# Target for linters
.PHONY: lint-update lint fmt-check fmt-fix spell-check spell-fix

lint-update: ## Update the linter version (removes previous one and downloads a new one)
	@rm -f $(GOPATH)/bin/golangci-lint
	@curl -sfL "https://install.goreleaser.com/github.com/golangci/golangci-lint.sh" | sh -s -- -b $(GOPATH)/bin latest

lint: ## Run linter on whole project
	@([[ ! -f $(GOPATH)/bin/golangci-lint ]] && curl -sfL "https://install.goreleaser.com/github.com/golangci/golangci-lint.sh" | sh -s -- -b $(GOPATH)/bin latest) || true
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" lint

fmt-check: ## Check code formatting
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" fmt

fmt-fix: ## Fix code formatting
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" fmt --fix

spell-check: ## Run spell checker on the project
	@GOOS="" GO111MODULE=off go get -u github.com/client9/misspell/cmd/misspell
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" spell

spell-fix: ## Fix spell checking issues
	@GOOS="" GO111MODULE=off go get -u github.com/client9/misspell/cmd/misspell
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" spell --fix
