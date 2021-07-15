include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS)
SHELL := /bin/bash
PKGS = $(shell go list ./... | grep -v /vendor)
$(eval $(call golang-version-check,1.13))

test: $(PKGS)

$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

install_deps:
	go mod vendor
