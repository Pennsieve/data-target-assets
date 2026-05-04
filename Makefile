.PHONY: help build-asset build-timeseries build-all test clean \
        docker-build-asset docker-build-timeseries docker-build-all \
        docker-push-asset docker-push-timeseries docker-push-all

WORKING_DIR              ?= $(shell pwd)
ASSET_IMAGE_NAME         ?= pennsieve/data-target-assets
TIMESERIES_IMAGE_NAME    ?= pennsieve/data-target-timeseries
IMAGE_TAG                ?= latest

.DEFAULT: help

help:
	@echo "Make Help"
	@echo ""
	@echo "make build-asset             - build asset cmd binary locally"
	@echo "make build-timeseries        - build timeseries cmd binary locally"
	@echo "make build-all               - build every cmd binary locally"
	@echo "make test                    - run tests"
	@echo "make docker-build-asset      - build asset Docker image"
	@echo "make docker-build-timeseries - build timeseries Docker image"
	@echo "make docker-build-all        - build every Docker image"
	@echo "make docker-push-asset       - build and push asset Docker image"
	@echo "make docker-push-timeseries  - build and push timeseries Docker image"
	@echo "make docker-push-all         - build and push every Docker image (CI entrypoint)"
	@echo "make clean                   - remove build artifacts"

build-asset:
	@echo "Building data-target-asset..."
	go build -o $(WORKING_DIR)/bin/data-target-asset $(WORKING_DIR)/cmd/asset

build-timeseries:
	@echo "Building data-target-timeseries..."
	go build -o $(WORKING_DIR)/bin/data-target-timeseries $(WORKING_DIR)/cmd/timeseries

build-all: build-asset build-timeseries

test:
	go test -v ./...

docker-build-asset:
	@echo "Building $(ASSET_IMAGE_NAME):$(IMAGE_TAG)..."
	DOCKER_BUILDKIT=1 docker build \
		--platform=linux/amd64 \
		-f Dockerfile.asset \
		-t $(ASSET_IMAGE_NAME):$(IMAGE_TAG) \
		-t $(ASSET_IMAGE_NAME):latest \
		$(WORKING_DIR)

docker-build-timeseries:
	@echo "Building $(TIMESERIES_IMAGE_NAME):$(IMAGE_TAG)..."
	DOCKER_BUILDKIT=1 docker build \
		--platform=linux/amd64 \
		-f Dockerfile.timeseries \
		-t $(TIMESERIES_IMAGE_NAME):$(IMAGE_TAG) \
		-t $(TIMESERIES_IMAGE_NAME):latest \
		$(WORKING_DIR)

docker-build-all: docker-build-asset docker-build-timeseries

docker-push-asset: docker-build-asset
	docker push $(ASSET_IMAGE_NAME):$(IMAGE_TAG)
	docker push $(ASSET_IMAGE_NAME):latest

docker-push-timeseries: docker-build-timeseries
	docker push $(TIMESERIES_IMAGE_NAME):$(IMAGE_TAG)
	docker push $(TIMESERIES_IMAGE_NAME):latest

docker-push-all: docker-push-asset docker-push-timeseries

clean:
	rm -rf $(WORKING_DIR)/bin
