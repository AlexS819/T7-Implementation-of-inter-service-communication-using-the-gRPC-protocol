// Package interceptor містить наскрізні gRPC-перехоплювачі (interceptors):
// автентифікацію сервісних клієнтів за API-ключем у Metadata та
// структуроване логування з хронометражем кожного виклику.
package interceptor

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// APIKeyHeader — ім'я метаданих (HTTP/2-заголовка), у якому клієнт передає ключ.
const APIKeyHeader = "x-api-key"

// AuthLogger — перехоплювач, що поєднує автентифікацію та логування.
type AuthLogger struct {
	apiKey string
	logger *log.Logger
}

// NewAuthLogger створює перехоплювач із конфігураційним сервісним ключем.
func NewAuthLogger(apiKey string, logger *log.Logger) *AuthLogger {
	return &AuthLogger{apiKey: apiKey, logger: logger}
}

// Unary повертає grpc.UnaryServerInterceptor для Unary RPC.
func (a *AuthLogger) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()

		// 1. Автентифікація ДО виклику бізнес-логіки.
		if err := a.authorize(ctx); err != nil {
			a.logCall("Unary", info.FullMethod, ctx, "REJECTED", err, time.Since(start))
			return nil, err
		}

		// 2. Виклик хендлера та хронометраж.
		resp, err := handler(ctx, req)
		a.logCall("Unary", info.FullMethod, ctx, "SUCCESS", err, time.Since(start))
		return resp, err
	}
}

// Stream повертає grpc.StreamServerInterceptor для потокових RPC.
// Без нього TrackProgress залишився б незахищеним, адже Unary-перехоплювач
// на потокові виклики не поширюється.
func (a *AuthLogger) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		if err := a.authorize(ss.Context()); err != nil {
			a.logCall("Stream", info.FullMethod, ss.Context(), "REJECTED", err, time.Since(start))
			return err
		}

		err := handler(srv, ss)
		a.logCall("Stream", info.FullMethod, ss.Context(), "SUCCESS", err, time.Since(start))
		return err
	}
}

// authorize перевіряє наявність та коректність сервісного ключа в Metadata.
func (a *AuthLogger) authorize(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "Invalid or missing API key")
	}

	keys := md.Get(APIKeyHeader)
	// md.Get повертає []string: один заголовок може бути переданий кілька разів.
	if len(keys) == 0 || keys[0] != a.apiKey {
		return status.Error(codes.Unauthenticated, "Invalid or missing API key")
	}

	return nil
}

// logCall виводить один структурований лог-рядок про завершений виклик.
func (a *AuthLogger) logCall(kind, method string, ctx context.Context, auth string, err error, duration time.Duration) {
	a.logger.Printf("[INTERCEPTOR] %s call Method=%s Client=%s Auth=%s Status=%s Duration=%s",
		kind, method, clientAddr(ctx), auth, status.Code(err), duration)
}

// clientAddr дістає мережеву адресу клієнта з контексту.
func clientAddr(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "unknown"
	}
	return p.Addr.String()
}
