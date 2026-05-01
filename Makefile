.PHONY: help build build-all docker-build docker-push clean test

SERVICE_NAME  := data-target-assets
IMAGE_NAME    := pennsieve/$(SERVICE_NAME)
IMAGE_TAG     ?= latest
WORKING_DIR   ?= $(shell pwd)

# TARGET selects which cmd/ binary to build (e.g. asset, timeseries).
TARGET ?= asset

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make build [TARGET=asset]   - build a single cmd binary locally"
	@echo "make build-all              - build every cmd binary locally"
	@echo "make test                   - run tests"
	@echo "make docker-build           - build Docker image (TARGET=asset by default)"
	@echo "make docker-push            - build and push Docker image"
	@echo "make clean                  - remove build artifacts"

build:
	@echo "Building $(SERVICE_NAME)..."
	go build -o $(WORKING_DIR)/$(SERVICE_NAME) $(WORKING_DIR)/cmd/$(TARGET)
	@echo "Done: $(SERVICE_NAME)"

build-all:
	@for d in $(WORKING_DIR)/cmd/*/; do \
		t=$$(basename $$d); \
		echo "Building $$t..."; \
		go build -o $(WORKING_DIR)/bin/$$t $(WORKING_DIR)/cmd/$$t || exit 1; \
	done
	@echo "Done: all binaries in bin/"

test:
	go test -v ./...

docker-build:
	@echo "Building Docker image $(IMAGE_NAME):$(IMAGE_TAG)..."
	DOCKER_BUILDKIT=1 docker build \
		--platform=linux/amd64 \
		--build-arg TARGET=$(TARGET) \
		-t $(IMAGE_NAME):$(IMAGE_TAG) \
		-t $(IMAGE_NAME):latest \
		$(WORKING_DIR)
	@echo "Done: $(IMAGE_NAME):$(IMAGE_TAG)"

docker-push: docker-build
	docker push $(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(IMAGE_NAME):latest

clean:
	rm -f $(WORKING_DIR)/$(SERVICE_NAME)
	rm -rf $(WORKING_DIR)/bin
