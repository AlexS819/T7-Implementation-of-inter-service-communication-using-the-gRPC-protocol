// Package storage реалізує потокобезпечне збереження завдань обробки медіа
// у пам'яті процесу. Сховище відокремлене від gRPC-шару, тому його можна
// замінити на Redis/PostgreSQL без зміни сервісної логіки.
package storage

import (
	"sync"
)

// Статуси життєвого циклу завдання.
const (
	StatusQueued     = "QUEUED"
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusCanceled   = "CANCELED"
)

// Job — внутрішня модель завдання обробки медіафайлу.
// Це не protobuf-структура: доменна модель навмисно відв'язана від контракту.
type Job struct {
	ID           string
	ClientID     string
	Filename     string
	TargetFormat string
	QualityLevel int32
	Status       string
	Progress     int32
	CreatedAt    int64 // Unix timestamp, секунди
	CompletedAt  int64 // Unix timestamp завершення, 0 — ще не завершено
}

// Store — сховище завдань, захищене sync.RWMutex.
// RWMutex обраний тому, що читань (GetJobStatus) значно більше, ніж записів:
// паралельні читачі не блокують одне одного.
type Store struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

// New створює порожнє сховище.
func New() *Store {
	return &Store{jobs: make(map[string]Job)}
}

// Create додає нове завдання. Якщо завдання з таким ID вже існує — перезаписує.
func (s *Store) Create(job Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

// Get повертає копію завдання за ідентифікатором.
// Копія (а не вказівник) унеможливлює гонку даних: клієнти сховища
// не можуть змінити стан в обхід блокування.
func (s *Store) Get(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

// Update атомарно змінює завдання через передану функцію-мутатор і
// повертає його оновлену копію. Якщо завдання не знайдено — ok == false.
func (s *Store) Update(id string, mutate func(job *Job)) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	mutate(&job)
	s.jobs[id] = job
	return job, true
}

// SetProgress оновлює відсоток виконання та статус завдання.
func (s *Store) SetProgress(id string, percent int32, jobStatus string) (Job, bool) {
	return s.Update(id, func(job *Job) {
		job.Progress = percent
		job.Status = jobStatus
	})
}

// Complete позначає завдання як успішно завершене.
func (s *Store) Complete(id string, completedAt int64) (Job, bool) {
	return s.Update(id, func(job *Job) {
		job.Status = StatusCompleted
		job.Progress = 100
		job.CompletedAt = completedAt
	})
}

// Cancel позначає завдання як скасоване (клієнт розірвав потік).
func (s *Store) Cancel(id string) (Job, bool) {
	return s.Update(id, func(job *Job) {
		job.Status = StatusCanceled
	})
}

// Count повертає кількість завдань у сховищі.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs)
}
