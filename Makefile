SHELL := /bin/bash

GOBIN := $(PWD)/bin
PATH := $(GOBIN):$(PATH)

CLUSTER_NAME ?= kubejob-cluster
KUBECONFIG ?= $(CURDIR)/.kube/config
export KUBECONFIG
export GOBIN

.PHONY: tools
tools:
	go install sigs.k8s.io/kind@v0.22.0
	go install github.com/bufbuild/buf/cmd/buf@v1.30.0
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

cluster/create: tools
	@{ \
	set -e ;\
	if [ "$$(kind get clusters --quiet | grep $(CLUSTER_NAME))" = "" ]; then \
		$(GOBIN)/kind create cluster --name $(CLUSTER_NAME) --config testdata/config/cluster.yaml ;\
	fi ;\
	}

cluster/delete:
	$(GOBIN)/kind delete clusters $(CLUSTER_NAME)

.PHONY: deploy
deploy: cluster/create deploy/image
	kubectl apply -f testdata/config/manifest.yaml

deploy/image:
	docker build --progress plain -f Dockerfile --target agent . -t 'kubejob:latest'
	$(GOBIN)/kind load docker-image --name $(CLUSTER_NAME) 'kubejob:latest'

.PHONY: wait
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

.PHONY: test
test:
	{ \
	set -e ;\
	while true; do \
		POD_NAME=$$(KUBECONFIG=$(KUBECONFIG) kubectl get pod | grep Running | grep kubejob-deployment | awk '{print $$1}'); \
		if [ "$$POD_NAME" != "" ]; then \
			kubectl exec -it $$POD_NAME -- go test -v -coverprofile=coverage.out ./ -count=1 -timeout 0; \
			exit $$?; \
		fi; \
		sleep 1; \
	done; \
	}

.PHONY: test-run
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

.PHONY: generate
generate: generate/buf

generate/buf:
	$(GOBIN)/buf generate
