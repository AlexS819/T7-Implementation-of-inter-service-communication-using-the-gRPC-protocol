// Команда client — консольний gRPC-клієнт, що демонструє повний сценарій
// взаємодії з Media Processing Service:
//  1. відмову в доступі без метаданих автентифікації;
//  2. успішну реєстрацію завдання (Unary RPC);
//  3. читання потоку прогресу (Server Streaming RPC);
//  4. отримання фінального стану завдання (Unary RPC).
//
// Запуск:
//
//	go run ./cmd/client
//	go run ./cmd/client -addr localhost:50051 -api-key secret-media-token -file clip.mov -format MP4 -quality 80
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "media-service/proto"
)

const (
	unaryTimeout  = 5 * time.Second
	streamTimeout = 60 * time.Second
)

func main() {
	addr := flag.String("addr", "localhost:50051", "адреса gRPC-сервера")
	apiKey := flag.String("api-key", "secret-media-token", "сервісний API-ключ для metadata x-api-key")
	filename := flag.String("file", "input_video.mov", "ім'я вхідного файлу")
	format := flag.String("format", "MEDIA_FORMAT_MP4", "цільовий формат (PNG, JPEG, WEBP, MP4)")
	quality := flag.Int("quality", 80, "рівень якості 1..100")
	clientID := flag.String("client-id", "cli-demo-service", "ідентифікатор сервісу-клієнта")
	flag.Parse()

	targetFormat, err := parseMediaFormat(*format)
	if err != nil {
		fail("%v", err)
	}

	fmt.Println("=== Media Processing Service CLI Client ===")

	// grpc.NewClient створює "ліниве" з'єднання: фізичне підключення
	// відбудеться під час першого RPC. insecure.NewCredentials() означає
	// відсутність TLS — припустимо лише для локального/internal-контуру.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("failed to create gRPC client: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewMediaServiceClient(conn)

	// ---------- Крок 1. Негативний сценарій: виклик без метаданих ----------
	fmt.Println("\n[Step 1] Attempting RPC call WITHOUT authentication metadata...")
	request := &pb.ProcessMediaRequest{
		ClientId:     *clientID,
		Filename:     *filename,
		TargetFormat: targetFormat,
		QualityLevel: int32(*quality),
	}

	ctx, cancel := context.WithTimeout(context.Background(), unaryTimeout)
	_, err = client.SubmitJob(ctx, request)
	cancel()

	switch {
	case err == nil:
		// Такого бути не повинно: сервер без ключа має відмовити.
		fmt.Println("[WARN] Server accepted an unauthenticated call. Check the interceptor configuration!")
	case isStatusCode(err, codes.Unauthenticated):
		fmt.Printf("[ERROR] Server responded with gRPC error: %v\n", err)
		fmt.Println("[INFO] Authentication enforcement verified successfully.")
	default:
		fail("unexpected error on unauthenticated call: %v", err)
	}

	// ---------- Крок 2. Авторизований виклик SubmitJob ----------
	fmt.Printf("\n[Step 2] Sending SubmitJob request with valid metadata (%s: %s)...\n",
		"x-api-key", *apiKey)

	// metadata.NewOutgoingContext прикріплює ключ до всіх RPC цього контексту.
	authCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-api-key", *apiKey))

	ctx, cancel = context.WithTimeout(authCtx, unaryTimeout)
	job, err := client.SubmitJob(ctx, request)
	cancel()
	if err != nil {
		failWithStatus("SubmitJob failed", err)
	}

	fmt.Println("[SUCCESS] Job created successfully:")
	fmt.Printf("  - Job ID:       %s\n", job.GetJobId())
	fmt.Printf("  - Initial State: %s\n", job.GetStatus())
	fmt.Printf("  - Submitted At:  %s\n", formatUnix(job.GetCreatedAt()))

	// ---------- Крок 3. Читання потоку прогресу ----------
	fmt.Println("\n[Step 3] Subscribing to Server Streaming: TrackProgress...")

	streamCtx, streamCancel := context.WithTimeout(authCtx, streamTimeout)
	defer streamCancel()

	stream, err := client.TrackProgress(streamCtx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	if err != nil {
		failWithStatus("TrackProgress failed", err)
	}

	for {
		update, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			// io.EOF — штатне завершення серверного потоку, а не помилка.
			break
		}
		if recvErr != nil {
			failWithStatus("stream receive failed", recvErr)
		}

		suffix := ""
		if update.GetIsCompleted() {
			suffix = "!"
		}
		fmt.Printf("  [%d%%] %s%s\n", update.GetProgressPercent(), update.GetCurrentStage(), suffix)
	}

	// ---------- Крок 4. Фінальний стан через Unary RPC ----------
	fmt.Println("\n[Step 4] Checking final job status via GetJobStatus RPC...")

	ctx, cancel = context.WithTimeout(authCtx, unaryTimeout)
	jobStatus, err := client.GetJobStatus(ctx, &pb.GetJobStatusRequest{JobId: job.GetJobId()})
	cancel()
	if err != nil {
		failWithStatus("GetJobStatus failed", err)
	}

	fmt.Printf("  - Current Status: %s\n", jobStatus.GetStatus())
	fmt.Printf("  - Progress:       %d%%\n", jobStatus.GetProgressPercent())
	fmt.Printf("  - Completed At:   %s\n", formatUnix(jobStatus.GetCompletedAt()))

	fmt.Println("\n=== Processing pipeline finished successfully ===")
}

// parseMediaFormat перетворює текстову назву формату у значення enum.
// Приймає як "MEDIA_FORMAT_MP4", так і коротке "mp4".
func parseMediaFormat(name string) (pb.MediaFormat, error) {
	normalized := strings.ToUpper(strings.TrimSpace(name))
	if !strings.HasPrefix(normalized, "MEDIA_FORMAT_") {
		normalized = "MEDIA_FORMAT_" + normalized
	}
	value, ok := pb.MediaFormat_value[normalized]
	if !ok {
		return pb.MediaFormat_MEDIA_FORMAT_UNSPECIFIED, fmt.Errorf("unknown media format %q", name)
	}
	return pb.MediaFormat(value), nil
}

// isStatusCode перевіряє, що помилка є gRPC-статусом із заданим кодом.
func isStatusCode(err error, want codes.Code) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == want
}

// failWithStatus друкує gRPC-статус помилки в зрозумілому вигляді та завершує роботу.
func failWithStatus(prefix string, err error) {
	st, ok := status.FromError(err)
	if !ok {
		fail("%s: %v", prefix, err)
	}
	fail("%s: code=%s message=%q", prefix, st.Code(), st.Message())
}

// formatUnix перетворює Unix timestamp у локальний час; 0 означає "не завершено".
func formatUnix(ts int64) string {
	if ts == 0 {
		return "-"
	}
	return time.Unix(ts, 0).Format("2006-01-02 15:04:05")
}

// fail друкує повідомлення в stderr і завершує програму з ненульовим кодом.
func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[FATAL] "+format+"\n", args...)
	os.Exit(1)
}
