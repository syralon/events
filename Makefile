.PHONY: proto
# generate proto codes
proto:
	protoc -I . --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. ./proto/syralon/events/*.proto