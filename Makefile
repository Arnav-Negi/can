can:
	@protoc --proto_path=./protofiles --go_out=. --go-grpc_out=. ./protofiles/can.proto

bootstrap:
	@protoc --proto_path=./protofiles --go_out=. --go-grpc_out=. ./protofiles/bootstrap.proto

all:
	make can
	make bootstrap