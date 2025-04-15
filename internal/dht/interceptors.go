package dht

import (
	"context"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"time"
)

// LoggingUnaryServerInterceptor returns a gRPC server interceptor for logging
func LoggingUnaryServerInterceptor(logger *logrus.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// Log incoming details
		fields := logrus.Fields{
			"method": info.FullMethod,
			"event":  "request",
		}

		// Extract client details
		if p, ok := peer.FromContext(ctx); ok {
			fields["client_address"] = p.Addr.String()
		}

		logger.WithFields(fields).Info("Handling gRPC request")

		// Invoke the original handler
		resp, err := handler(ctx, req)

		// Log response details
		duration := time.Since(start)
		resFields := logrus.Fields{
			"method":   info.FullMethod,
			"duration": duration.String(),
			"event":    "response",
		}

		if err != nil {
			resFields["error"] = err.Error()
			logger.WithFields(resFields).Error("gRPC request failed")
		} else {
			logger.WithFields(resFields).Info("gRPC request completed successfully")
		}

		return resp, err
	}
}

// LoggingUnaryClientInterceptor returns a gRPC client interceptor for logging
func LoggingUnaryClientInterceptor(logger *logrus.Logger) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		start := time.Now()

		// Log outgoing request details
		logger.WithFields(logrus.Fields{
			"method":   method,
			"event":    "request",
			"payload":  req,
			"call_opt": opts,
		}).Info("Making gRPC call")

		// Make the gRPC call
		err := invoker(ctx, method, req, reply, cc, opts...)

		// Log response details
		duration := time.Since(start)
		resFields := logrus.Fields{
			"method":   method,
			"duration": duration.String(),
			"event":    "response",
		}
		if err != nil {
			resFields["error"] = err.Error()
			logger.WithFields(resFields).Error("gRPC call failed")
		} else {
			resFields["response"] = reply
			logger.WithFields(resFields).Info("gRPC call completed successfully")
		}

		return err
	}
}
