package interceptor

import (
	"context"
	"io"
	"log"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testAPIKey = "secret-media-token"

func newDiscardLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

func TestAuthorize(t *testing.T) {
	auth := NewAuthLogger(testAPIKey, newDiscardLogger())

	tests := []struct {
		name    string
		ctx     context.Context
		wantErr bool
	}{
		{
			name:    "метадані відсутні",
			ctx:     context.Background(),
			wantErr: true,
		},
		{
			name:    "заголовок відсутній",
			ctx:     metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "1")),
			wantErr: true,
		},
		{
			name:    "невірний ключ",
			ctx:     metadata.NewIncomingContext(context.Background(), metadata.Pairs(APIKeyHeader, "wrong")),
			wantErr: true,
		},
		{
			name:    "коректний ключ",
			ctx:     metadata.NewIncomingContext(context.Background(), metadata.Pairs(APIKeyHeader, testAPIKey)),
			wantErr: false,
		},
		{
			name:    "ключ серед кількох значень",
			ctx:     metadata.NewIncomingContext(context.Background(), metadata.Pairs(APIKeyHeader, testAPIKey, APIKeyHeader, "extra")),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := auth.authorize(tt.ctx)
			if tt.wantErr && err == nil {
				t.Fatal("очікували помилку автентифікації")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("не очікували помилки, отримали: %v", err)
			}
			if tt.wantErr && status.Code(err) != codes.Unauthenticated {
				t.Fatalf("очікували код Unauthenticated, отримали: %v", status.Code(err))
			}
		})
	}
}

// TestUnaryInterceptorRejectsWithoutKey перевіряє, що захищений метод
// не викликається без валідного ключа.
func TestUnaryInterceptorRejectsWithoutKey(t *testing.T) {
	auth := NewAuthLogger(testAPIKey, newDiscardLogger())
	handlerCalled := false

	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/media.MediaService/SubmitJob"}

	_, err := auth.Unary()(context.Background(), nil, info, handler)
	if err == nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("очікували Unauthenticated, отримали: %v", err)
	}
	if handlerCalled {
		t.Fatal("хендлер не повинен викликатися без автентифікації")
	}

	// З коректним ключем хендлер має бути викликаний.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(APIKeyHeader, testAPIKey))
	if _, err := auth.Unary()(ctx, nil, info, handler); err != nil {
		t.Fatalf("не очікували помилки: %v", err)
	}
	if !handlerCalled {
		t.Fatal("хендлер має бути викликаний з валідним ключем")
	}
}
