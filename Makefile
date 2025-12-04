# Add .env file from .env-defaults to avoid makefile error
_:=$(shell [ ! -f .env ] && cp .env-defaults .env)

include .env

update:
	go get -u ./... && go mod tidy

lint:
	golangci-lint run

build:
	docker build --progress=plain -t template -f Dockerfile .

run:
	docker-compose up --build -d
