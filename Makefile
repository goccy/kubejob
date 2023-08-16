SHELL := /bin/bash

GOBIN := $(PWD)/bin
PATH := $(GOBIN):$(PATH)

CLUSTER_NAME ?= kubejob-cluster
KUBECONFIG ?= $(CURDIR)/.kube/config
export KUBECONFIG

KIND_VERSION := v0.20.0

kind/install:
	GOBIN=$(GOBIN) go install sigs.k8s.io/kind@$(KIND_VERSION)

cluster/create: kind/install
	@{ \
	set -e ;\
	if [ "$$(kind get clusters --quiet | grep $(CLUSTER_NAME))" = "" ]; then \
		$(GOBIN)/kind create cluster --name $(CLUSTER_NAME) --config testdata/config/cluster.yaml ;\
	fi ;\
	}

cluster/delete: kind/install
	$(GOBIN)/kind delete clusters $(CLUSTER_NAME)

deploy: cluster/create deploy/image
	kubectl apply -f testdata/config/manifest.yaml

deploy/image:
	docker build --progress plain -f Dockerfile --target agent . -t 'kubejob:latest'
	$(GOBIN)/kind load docker-image --name $(CLUSTER_NAME) 'kubejob:latest'

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

test-run:
	{ \
	set -e ;\
	while true; do \
		POD_NAME=$$(KUBECONFIG=$(KUBECONFIG) kubectl get pod | grep Running | grep kubejob-deployment | awk '{print $$1}'); \
		if [ "$$POD_NAME" != "" ]; then \
			kubectl exec -it $$POD_NAME -- go test -v -coverprofile=coverage.out ./ -count=1 -run $(TEST); \
			exit $$?; \
		fi; \
		sleep 1; \
	done; \
	}

generate: proto-gen

proto-gen:
	protoc ./agent/agent.proto --go_out=plugins=grpc:.
