can:
	@protoc --proto_path=./protofiles --go_out=. --go-grpc_out=. ./protofiles/can.proto

bootstrap:
	@protoc --proto_path=./protofiles --go_out=. --go-grpc_out=. ./protofiles/bootstrap.proto

root-cert:
	cd ./cmd/bootstrapper/certs/ && ./root-gen.sh

certs:
	cd ./cmd/cli/certs/ && ./gen.sh

clean:
	rm -rf ./cmd/cli/logs/*

all:
	make can
	make bootstrap