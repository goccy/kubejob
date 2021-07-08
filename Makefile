SHELL := /bin/bash

BIN := $(CURDIR)/.bin
PATH := $(abspath $(BIN)):$(PATH)

UNAME_OS := $(shell uname -s)

$(BIN):
	@mkdir -p $(BIN)

KIND := $(BIN)/kind
KIND_VERSION := v0.11.0
$(KIND): | $(BIN)
	@curl -sSLo $(KIND) "https://kind.sigs.k8s.io/dl/$(KIND_VERSION)/kind-$(UNAME_OS)-amd64"
	@chmod +x $(KIND)

CLUSTER_NAME ?= kubejob-cluster
KUBECONFIG ?= $(CURDIR)/.kube/config
export KUBECONFIG

test-cluster: $(KIND)
	@{ \
	set -e ;\
	if [ "$$(kind get clusters --quiet | grep $(CLUSTER_NAME))" = "" ]; then \
		$(KIND) create cluster --name $(CLUSTER_NAME) --config testdata/config/cluster.yaml ;\
	fi ;\
	}

delete-cluster: $(KIND)
	$(KIND) delete clusters $(CLUSTER_NAME)

deploy: test-cluster
	kubectl apply -f testdata/config/manifest.yaml

wait:
	{ \
	set -e ;\
	while true; do \
		POD_NAME=$$(KUBECONFIG=$(KUBECONFIG) kubectl get pod | grep Running | grep kubejob-deployment | awk '{print $$1}'); \
		if [ "$$POD_NAME" != "" ]; then \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	}

test:
	{ \
	set -e ;\
	while true; do \
		POD_NAME=$$(KUBECONFIG=$(KUBECONFIG) kubectl get pod | grep Running | grep kubejob-deployment | awk '{print $$1}'); \
		if [ "$$POD_NAME" != "" ]; then \
			kubectl exec -it $$POD_NAME -- go test -v -coverprofile=coverage.out ./ -count=1; \
			exit $$?; \
		fi; \
		sleep 1; \
	done; \
	}
