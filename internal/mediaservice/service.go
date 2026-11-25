// Package mediaservice реалізує серверну бізнес-логіку gRPC-сервісу
// обробки медіафайлів: реєстрацію завдання, читання його стану та
// потокову трансляцію прогресу (Server Streaming).
package mediaservice

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"media-service/internal/storage"
	pb "media-service/proto"
)

// Назви технологічних стадій, які емулюють реальний конвеєр обробки медіа.
const (
	StageDemuxing = "Demuxing input stream"
	StageEncoding = "Encoding video frames"
	StageRemuxing = "Remuxing audio and headers"
	StageDone     = "Process completed successfully"
)

// processingStage описує один крок імітованого конвеєра обробки.
type processingStage struct {
	percent int32
	name    string
}

// defaultStages — послідовність стадій обробки з наростаючим прогресом.
var defaultStages = []processingStage{
	{percent: 25, name: StageDemuxing},
	{percent: 50, name: StageEncoding},
	{percent: 75, name: StageRemuxing},
	{percent: 100, name: StageDone},
}

// Server реалізує згенерований інтерфейс pb.MediaServiceServer.
// Вбудована структура pb.UnimplementedMediaServiceServer дає зворотну
// сумісність: якщо в контракт додати новий метод, сервер не перестане
// компілюватися (метод поверне codes.Unimplemented).
type Server struct {
	pb.UnimplementedMediaServiceServer

	store      *storage.Store
	logger     *log.Logger
	stageDelay time.Duration
	newID      func() string // ін'єкція генератора ID (зручно для тестів)
}

// New створює серверне впровадження сервісу.
func New(store *storage.Store, logger *log.Logger, stageDelay time.Duration) *Server {
	return &Server{
		store:      store,
		logger:     logger,
		stageDelay: stageDelay,
		newID:      uuid.NewString,
	}
}

// SubmitJob (Unary RPC) реєструє нове завдання та повертає його ідентифікатор.
func (s *Server) SubmitJob(_ context.Context, req *pb.ProcessMediaRequest) (*pb.JobResponse, error) {
	if err := validateSubmitRequest(req); err != nil {
		return nil, err
	}

	now := time.Now()
	job := storage.Job{
		ID:           s.newID(),
		ClientID:     req.GetClientId(),
		Filename:     req.GetFilename(),
		TargetFormat: req.GetTargetFormat().String(),
		QualityLevel: req.GetQualityLevel(),
		Status:       storage.StatusQueued,
		Progress:     0,
		CreatedAt:    now.Unix(),
	}
	s.store.Create(job)

	s.logger.Printf("[STORAGE] Job registered: ID=%s Filename=%s TargetFormat=%s Status=%s",
		job.ID, job.Filename, job.TargetFormat, job.Status)

	return &pb.JobResponse{
		JobId:     job.ID,
		Status:    job.Status,
		CreatedAt: job.CreatedAt,
	}, nil
}

// GetJobStatus (Unary RPC) повертає поточний стан завдання.
func (s *Server) GetJobStatus(_ context.Context, req *pb.GetJobStatusRequest) (*pb.JobStatusResponse, error) {
	job, ok := s.store.Get(req.GetJobId())
	if !ok {
		// Канонічна gRPC-помилка: код NotFound + текст опису.
		return nil, status.Errorf(codes.NotFound, "job %q not found", req.GetJobId())
	}

	return &pb.JobStatusResponse{
		JobId:           job.ID,
		Status:          job.Status,
		ProgressPercent: job.Progress,
		CompletedAt:     job.CompletedAt,
	}, nil
}

// TrackProgress (Server Streaming RPC) надсилає клієнту послідовність
// оновлень прогресу. Реальна обробка медіа імітується таймером.
func (s *Server) TrackProgress(req *pb.GetJobStatusRequest, stream pb.MediaService_TrackProgressServer) error {
	jobID := req.GetJobId()

	job, ok := s.store.Get(jobID)
	if !ok {
		return status.Errorf(codes.NotFound, "job %q not found", jobID)
	}

	s.logger.Printf("[STREAM] Client connected to track progress for JobID=%s", jobID)

	// Якщо завдання вже завершене — немає сенсу чекати таймерів:
	// віддаємо фінальний стан одним повідомленням і закриваємо потік.
	if job.Status == storage.StatusCompleted {
		return stream.Send(&pb.ProgressUpdate{
			JobId:           jobID,
			CurrentStage:    StageDone,
			ProgressPercent: 100,
			IsCompleted:     true,
		})
	}

	for i, stage := range defaultStages {
		// Ключовий момент: чекаємо або таймер, або скасування контексту.
		// Без цієї гілки горутина обробки "висіла" б після розриву з'єднання.
		select {
		case <-stream.Context().Done():
			s.logger.Printf("[STREAM] Client disconnected, tracking stopped for JobID=%s: %v",
				jobID, stream.Context().Err())
			s.store.Cancel(jobID)
			return status.Error(codes.Canceled, "progress tracking canceled by client")
		case <-time.After(s.stageDelay):
		}

		isCompleted := stage.percent == 100
		if i == 0 {
			// Перша стадія переводить завдання зі стану QUEUED у PROCESSING.
			s.store.SetProgress(jobID, stage.percent, storage.StatusProcessing)
		} else if isCompleted {
			s.store.Complete(jobID, time.Now().Unix())
		} else {
			s.store.SetProgress(jobID, stage.percent, storage.StatusProcessing)
		}

		update := &pb.ProgressUpdate{
			JobId:           jobID,
			CurrentStage:    stage.name,
			ProgressPercent: stage.percent,
			IsCompleted:     isCompleted,
		}
		if err := stream.Send(update); err != nil {
			// stream.Send повертає помилку, якщо клієнт уже відключився.
			s.logger.Printf("[STREAM] Failed to send update for JobID=%s: %v", jobID, err)
			s.store.Cancel(jobID)
			return err
		}

		s.logger.Printf("[STREAM] JobID=%s -> Sent stage %q (%d%%)", jobID, stage.name, stage.percent)
	}

	s.logger.Printf("[STREAM] Progress tracking finished for JobID=%s", jobID)
	return nil
}

// validateSubmitRequest перевіряє вхідні дані на рівні бізнес-логіки.
// Помилки валідації повертаються з кодом codes.InvalidArgument.
func validateSubmitRequest(req *pb.ProcessMediaRequest) error {
	if req.GetClientId() == "" {
		return status.Error(codes.InvalidArgument, "client_id must not be empty")
	}
	if req.GetFilename() == "" {
		return status.Error(codes.InvalidArgument, "filename must not be empty")
	}
	if req.GetTargetFormat() == pb.MediaFormat_MEDIA_FORMAT_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "target_format must be specified")
	}
	if q := req.GetQualityLevel(); q < 1 || q > 100 {
		return status.Errorf(codes.InvalidArgument, "quality_level must be in range 1..100, got %d", q)
	}
	return nil
}
