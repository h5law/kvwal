SHELL := /bin/bash

.SILENT:

#####################
###    General    ###
#####################

.PHONY: help
.DEFAULT_GOAL := help
help:  ## Prints all the targets in all the Makefiles
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: list
list:  ## List all make targets
	@${MAKE} -pRrn : -f $(MAKEFILE_LIST) 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | sort

## Ensure godoc is installed
.PHONY: check_godoc
# Internal helper target - check if godoc is installed
check_godoc:
	{ \
	if ( ! ( command -v godoc >/dev/null )); then \
		echo "Seems like you don't have godoc installed. Make sure you install it via 'go install golang.org/x/tools/cmd/godoc@latest' before continuing"; \
		exit 1; \
	fi; \
	}

#####################
### Documentation ###
#####################

.PHONY: go_docs
go_docs: check_godoc ## Generate documentation for the project
	echo "Visit http://localhost:6060/pkg/github.com/h5law/kvwal"
	godoc -http=:6060

#####################
####   Testing   ####
#####################

.PHONY: test_all
test_all:  ## runs the test suite
	go test -p 1 ./... -mod=readonly -race

.PHONY: test_all_clean
test_all_clean:  ## runs the test suite after cleaning the test cache
	go clean -testcache && go test -v -p 1 ./... -mod=readonly -race

.PHONY: test_all_verbose
test_all_verbose:  ## runs the test suite with verbose output
	go test -v -p 1 ./... -mod=readonly -race
