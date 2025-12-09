package events

import (
	"context"
	"google.golang.org/grpc/metadata"
	"strings"
)

func OutgoingMetadata(ctx context.Context) map[string]string {
	md := make(map[string]string)
	if outgoingMD, ok := metadata.FromOutgoingContext(ctx); ok {
		for k := range outgoingMD {
			md[k] = strings.Join(outgoingMD[k], ",")
		}
	}
	return md
}

func IncomingMetadata(ctx context.Context) map[string]string {
	md := make(map[string]string)
	if outgoingMD, ok := metadata.FromIncomingContext(ctx); ok {
		for k := range outgoingMD {
			md[k] = strings.Join(outgoingMD[k], ",")
		}
	}
	return md
}
