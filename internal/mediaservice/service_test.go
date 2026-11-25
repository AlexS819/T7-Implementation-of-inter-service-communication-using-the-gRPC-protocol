package mediaservice

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"media-service/internal/storage"
	pb "media-service/proto"
)

// fakeTrackStream — тестова реалізація серверного потоку.
// Вбудований grpc.ServerStream закриває методи, які тест не використовує.
type fakeTrackStream struct {
	grpc.ServerStream
	ctx     context.Context
	updates []*pb.ProgressUpdate
}

func (f *fakeTrackStream) Context() context.Context { return f.ctx }

func (f *fakeTrackStream) Send(update *pb.ProgressUpdate) error {
	f.updates = append(f.updates, update)
	return nil
}

func newTestServer() *Server {
	return New(storage.New(), log.New(io.Discard, "", 0), time.Millisecond)
}

func validRequest() *pb.ProcessMediaRequest {
	return &pb.ProcessMediaRequest{
		ClientId:     "test-client",
		Filename:     "input.mov",
		TargetFormat: pb.MediaFormat_MEDIA_FORMAT_MP4,
		QualityLevel: 80,
	}
}

func TestSubmitJobCreatesQueuedJob(t *testing.T) {
	srv := newTestServer()

	resp, err := srv.SubmitJob(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("не очікували помилки: %v", err)
	}
	if resp.GetJobId() == "" {
		t.Fatal("job_id не повинен бути порожнім")
	}
	if resp.GetStatus() != storage.StatusQueued {
		t.Fatalf("очікували статус QUEUED, отримали %q", resp.GetStatus())
	}
	if resp.GetCreatedAt() == 0 {
		t.Fatal("created_at повинен бути заповнений")
	}
}

func TestSubmitJobValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*pb.ProcessMediaRequest)
	}{
		{"порожній client_id", func(r *pb.ProcessMediaRequest) { r.ClientId = "" }},
		{"порожній filename", func(r *pb.ProcessMediaRequest) { r.Filename = "" }},
		{"не вказаний формат", func(r *pb.ProcessMediaRequest) { r.TargetFormat = pb.MediaFormat_MEDIA_FORMAT_UNSPECIFIED }},
		{"quality_level = 0", func(r *pb.ProcessMediaRequest) { r.QualityLevel = 0 }},
		{"quality_level = 101", func(r *pb.ProcessMediaRequest) { r.QualityLevel = 101 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validRequest()
			tt.mutate(req)

			_, err := newTestServer().SubmitJob(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("очікували InvalidArgument, отримали: %v", err)
			}
		})
	}
}

func TestGetJobStatusNotFound(t *testing.T) {
	_, err := newTestServer().GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("очікували NotFound, отримали: %v", err)
	}
}

func TestTrackProgressCompletesJob(t *testing.T) {
	srv := newTestServer()

	resp, err := srv.SubmitJob(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	stream := &fakeTrackStream{ctx: context.Background()}
	if err := srv.TrackProgress(&pb.GetJobStatusRequest{JobId: resp.GetJobId()}, stream); err != nil {
		t.Fatalf("TrackProgress: %v", err)
	}

	if len(stream.updates) != len(defaultStages) {
		t.Fatalf("очікували %d оновлень, отримали %d", len(defaultStages), len(stream.updates))
	}
	last := stream.updates[len(stream.updates)-1]
	if !last.GetIsCompleted() || last.GetProgressPercent() != 100 {
		t.Fatalf("останнє оновлення має бути 100%% та is_completed=true: %+v", last)
	}

	// Після потоку стан у сховищі має стати COMPLETED.
	final, err := srv.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: resp.GetJobId()})
	if err != nil {
		t.Fatalf("GetJobStatus: %v", err)
	}
	if final.GetStatus() != storage.StatusCompleted || final.GetProgressPercent() != 100 {
		t.Fatalf("очікували COMPLETED/100, отримали %s/%d", final.GetStatus(), final.GetProgressPercent())
	}
	if final.GetCompletedAt() == 0 {
		t.Fatal("completed_at повинен бути заповнений")
	}
}

func TestTrackProgressCanceledByClient(t *testing.T) {
	srv := newTestServer()

	resp, err := srv.SubmitJob(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // імітуємо розрив з'єднання до початку обробки

	stream := &fakeTrackStream{ctx: ctx}
	err = srv.TrackProgress(&pb.GetJobStatusRequest{JobId: resp.GetJobId()}, stream)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("очікували Canceled, отримали: %v", err)
	}
}

func TestTrackProgressUnknownJob(t *testing.T) {
	stream := &fakeTrackStream{ctx: context.Background()}
	err := newTestServer().TrackProgress(&pb.GetJobStatusRequest{JobId: "missing"}, stream)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("очікували NotFound, отримали: %v", err)
	}
}
