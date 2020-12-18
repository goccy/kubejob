SHELL := /bin/bash

BIN := $(CURDIR)/.bin
PATH := $(abspath $(BIN)):$(PATH)

UNAME_OS := $(shell uname -s)

$(BIN):
	@mkdir -p $(BIN)

KIND := $(BIN)/kind
KIND_VERSION := v0.8.1
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

build-image:
	docker build -t kubejob:latest .

delete-image:
	docker image rm -f kubejob:latest

upload-image: build-image
	@$(KIND) load --name $(TEST_CLUSTER_NAME) docker-image kubejob:latest

deploy: test-cluster
	kubectl apply -f testdata/config/manifest.yaml
	kubectl apply -f https://docs.projectcalico.org/v3.8/manifests/calico.yaml

test:
	{ \
	set -e ;\
	while true; do \
		podname=$(shell KUBECONFIG=$(KUBECONFIG) kubectl get pod | grep Running | grep kubejob-deployment | awk '{print $$1}'); \
		if [ "$$podname" != "" ]; then \
			kubectl exec -it $$podname -- go test -v ./ -count=1; \
			exit $$?; \
		fi; \
		sleep 1; \
	done; \
	}
