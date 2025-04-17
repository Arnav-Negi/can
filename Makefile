can:
	@protoc --proto_path=./protofiles --go_out=. --go-grpc_out=. ./protofiles/can.proto

bootstrap:
	@protoc --proto_path=./protofiles --go_out=. --go-grpc_out=. ./protofiles/bootstrap.proto

root-cert:
	cd ./cmd/bootstrapper/certs/ && ./root-gen.sh

certs:
	cd ./cmd/cli/certs/ && ./gen.sh

clean:
	- rm -rf ./cmd/cli/logs
	- rm -rf ./cmd/cli/certs
	- rm -rf ./testing/scale/logs
	- rm -rf ./testing/scale/certs

all:
	make can
	make bootstrap