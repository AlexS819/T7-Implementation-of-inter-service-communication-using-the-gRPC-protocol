// Команда server запускає gRPC-сервіс обробки медіафайлів.
//
// Запуск:
//
//	go run ./cmd/server
//	go run ./cmd/server -addr :50051 -api-key secret-media-token -stage-delay 800ms
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"media-service/internal/interceptor"
	"media-service/internal/mediaservice"
	"media-service/internal/storage"
	pb "media-service/proto"
)

// defaultAPIKey — сервісний токен за замовчуванням (для навчального середовища).
const defaultAPIKey = "secret-media-token"

func main() {
	// Конфігурація через прапорці CLI; ключ можна передати через змінну середовища,
	// щоб не тримати секрет у командному рядку чи в репозиторії.
	addr := flag.String("addr", ":50051", "адреса та порт gRPC-сервера")
	apiKey := flag.String("api-key", envOrDefault("MEDIA_API_KEY", defaultAPIKey), "сервісний API-ключ (metadata x-api-key)")
	stageDelay := flag.Duration("stage-delay", 800*time.Millisecond, "затримка між стадіями імітованої обробки")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags)

	// Композиція шарів: сховище -> сервіс -> gRPC-сервер із перехоплювачами.
	store := storage.New()
	service := mediaservice.New(store, logger, *stageDelay)
	authLogger := interceptor.NewAuthLogger(*apiKey, logger)

	server := grpc.NewServer(
		grpc.UnaryInterceptor(authLogger.Unary()),
		grpc.StreamInterceptor(authLogger.Stream()),
	)

	pb.RegisterMediaServiceServer(server, service)
	// gRPC Reflection дозволяє Postman/grpcurl досліджувати контракт без .proto файлу.
	reflection.Register(server)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Fatalf("[SERVER] Failed to listen on %s: %v", *addr, err)
	}

	logger.Printf("[SERVER] Starting Media Processing gRPC service on %s...", *addr)
	logger.Printf("[SERVER] Authentication enabled: header %q is required for every RPC", interceptor.APIKeyHeader)

	// Коректна зупинка: GracefulStop дочікується завершення активних викликів,
	// замість того щоб обірвати з'єднання (і потоки) посеред роботи.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		logger.Println("[SERVER] Shutdown signal received, stopping gracefully...")
		server.GracefulStop()
	}()

	if err := server.Serve(listener); err != nil {
		logger.Fatalf("[SERVER] gRPC server terminated unexpectedly: %v", err)
	}
	logger.Println("[SERVER] gRPC server stopped")
}

// envOrDefault повертає значення змінної середовища або fallback.
func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
