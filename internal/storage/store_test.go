package storage

import (
	"fmt"
	"sync"
	"testing"
)

// newTestJob повертає завдання-заготовку для тестів.
func newTestJob(id string) Job {
	return Job{
		ID:           id,
		ClientID:     "test-client",
		Filename:     "input.mov",
		TargetFormat: "MEDIA_FORMAT_MP4",
		QualityLevel: 80,
		Status:       StatusQueued,
	}
}

func TestCreateAndGet(t *testing.T) {
	s := New()
	job := newTestJob("job-1")
	s.Create(job)

	got, ok := s.Get("job-1")
	if !ok {
		t.Fatal("очікували знайдене завдання, отримали ok == false")
	}
	if got.ID != job.ID || got.Status != StatusQueued {
		t.Fatalf("отримали %+v, очікували %+v", got, job)
	}
}

func TestGetMissingReturnsFalse(t *testing.T) {
	s := New()
	if _, ok := s.Get("does-not-exist"); ok {
		t.Fatal("для відсутнього завдання очікували ok == false")
	}
}

func TestUpdateChangesState(t *testing.T) {
	s := New()
	s.Create(newTestJob("job-1"))

	updated, ok := s.SetProgress("job-1", 50, StatusProcessing)
	if !ok {
		t.Fatal("очікували успішне оновлення")
	}
	if updated.Progress != 50 || updated.Status != StatusProcessing {
		t.Fatalf("некоректний стан після оновлення: %+v", updated)
	}

	completed, ok := s.Complete("job-1", 1700000000)
	if !ok {
		t.Fatal("очікували успішне завершення")
	}
	if completed.Status != StatusCompleted || completed.Progress != 100 || completed.CompletedAt != 1700000000 {
		t.Fatalf("некоректний фінальний стан: %+v", completed)
	}
}

func TestUpdateMissingJob(t *testing.T) {
	s := New()
	if _, ok := s.Update("missing", func(job *Job) { job.Status = StatusCompleted }); ok {
		t.Fatal("для відсутнього завдання очікували ok == false")
	}
}

// TestConcurrentAccess перевіряє відсутність гонок даних.
// Запускати з прапорцем -race: go test -race ./...
func TestConcurrentAccess(t *testing.T) {
	s := New()
	const workers = 50

	var wg sync.WaitGroup
	wg.Add(workers * 2)

	for i := 0; i < workers; i++ {
		go func(n int) {
			defer wg.Done()
			s.Create(newTestJob(fmt.Sprintf("job-%d", n)))
		}(i)
		go func(n int) {
			defer wg.Done()
			s.SetProgress(fmt.Sprintf("job-%d", n), 25, StatusProcessing)
		}(i)
	}

	wg.Wait()

	if got := s.Count(); got != workers {
		t.Fatalf("очікували %d завдань, отримали %d", workers, got)
	}
}
