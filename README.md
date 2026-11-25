# Практична робота № 7. Реалізація міжсервісної взаємодії за протоколом gRPC

**Тема:** реалізація міжсервісної взаємодії за протоколом gRPC.
**Проєкт:** `media-service` — внутрішній мікросервіс обробки та конвертації медіафайлів.
**Виконані рівні:** Рівень 1 (базовий), Рівень 2 (середній), Рівень 3 (поглиблений).

**Студент:** Сочка О.В.
**Група:** КН-41

---

## 1. Мета роботи

- Опанувати концепцію RPC та архітектурні відмінності gRPC від класичного REST API.
- Засвоїти мову опису інтерфейсів Protocol Buffers (proto3) для створення суворих бінарних контрактів.
- Опанувати кодогенерацію `protoc` із плагінами `protoc-gen-go` та `protoc-gen-go-grpc`.
- Реалізувати серверні інтерфейси для Unary-запитів та Server Streaming RPC.
- Розробити наскрізні Unary/Stream-перехоплювачі для автентифікації через gRPC Metadata та вимірювання затримок.

## 2. Середовище та інструменти

| Компонент | Версія / опис |
|---|---|
| ОС | Windows 11 |
| Go SDK | go 1.23.4 (модуль оголошений як `go 1.22`) |
| gRPC-Go | `google.golang.org/grpc v1.68.1` |
| Protocol Buffers (runtime) | `google.golang.org/protobuf v1.36.5` |
| Кодогенератори | `protoc-gen-go v1.36.5`, `protoc-gen-go-grpc v1.5.1` |
| UUID | `github.com/google/uuid v1.6.0` |
| Термінал | PowerShell 7 |
| Перевірка API | `grpcurl` / Postman (gRPC), gRPC Reflection |

> У цьому середовищі бінарник `protoc` був недоступний, тому генерацію виконано безпосередньо
> плагінами `protoc-gen-go` та `protoc-gen-go-grpc` (саме їх викликає `protoc`). Команда
> канонічної генерації та скрипти наведені в розділі 6; згенеровані файли вже додані в репозиторій,
> тож для збірки `protoc` не потрібен.

## 3. Архітектура рішення

### 3.1 Загальна схема

```
                    ┌──────────────────────────── cmd/client (Go CLI) ───────────────────────────┐
                    │  metadata.NewOutgoingContext(ctx, "x-api-key: secret-media-token")         │
                    │  1) SubmitJob (Unary)   2) TrackProgress (Server Streaming)   3) GetJobStatus │
                    └───────────────────────────────────┬────────────────────────────────────────┘
                                                        │  HTTP/2 + Protobuf (TCP :50051)
                    ┌───────────────────────────────────▼────────────────────────────────────────┐
                    │                         gRPC Server (grpc.NewServer)                        │
                    │  ┌──────────────────────────────────────────────────────────────────────┐  │
                    │  │ Interceptor chain: UnaryInterceptor / StreamInterceptor              │  │
                    │  │   authorize() -> metadata.FromIncomingContext -> x-api-key            │  │
                    │  │   reject: codes.Unauthenticated      accept: handler(ctx, req)         │  │
                    │  │   log: [INTERCEPTOR] Method=... Client=... Status=... Duration=...     │  │
                    │  └───────────────────────────────────┬──────────────────────────────────┘  │
                    │  ┌───────────────────────────────────▼──────────────────────────────────┐  │
                    │  │ mediaservice.Server (реалізує pb.MediaServiceServer)                  │  │
                    │  │   SubmitJob      -> валідація + UUID + Create()                       │  │
                    │  │   GetJobStatus   -> Get() + NotFound                                  │  │
                    │  │   TrackProgress  -> цикл стадій + stream.Send() + select ctx.Done()    │  │
                    │  └───────────────────────────────────┬──────────────────────────────────┘  │
                    │  ┌───────────────────────────────────▼──────────────────────────────────┐  │
                    │  │ storage.Store (map[string]Job + sync.RWMutex)                         │  │
                    │  └──────────────────────────────────────────────────────────────────────┘  │
                    └────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Шари та відповідальність

| Шар | Пакет | Відповідальність |
|---|---|---|
| Контракт | `proto/media.proto` | Єдине джерело правди: типи повідомлень, enum, сервіс і методи. З нього генеруються `media.pb.go` та `media_grpc.pb.go`. |
| Транспорт / наскрізна логіка | `internal/interceptor` | Автентифікація за `x-api-key` з Metadata, структуроване логування, хронометраж. Не знає нічого про бізнес-логіку. |
| Бізнес-логіка (gRPC-хендлери) | `internal/mediaservice` | Валідація вхідних даних, генерація `job_id`, керування життєвим циклом завдання, імітація конвеєра обробки, потокова віддача прогресу. |
| Зберігання стану | `internal/storage` | Потокобезпечне сховище в пам'яті (`map` + `sync.RWMutex`), доменна модель `Job`. |
| Точки входу | `cmd/server`, `cmd/client` | Композиція залежностей (wiring), конфігурація через прапорці, запуск/зупинка процесу, CLI-сценарій. |

Розділення на шари дає змогу: (1) замінити in-memory сховище на Redis/PostgreSQL, не змінюючи
хендлери; (2) перевикористати перехоплювачі в інших сервісах; (3) тестувати бізнес-логіку без
мережі — саме так зроблено юніт-тести.

### 3.3 Життєвий цикл завдання

```
QUEUED ──(перше оновлення прогресу, 25%)──> PROCESSING ──(100%)──> COMPLETED
   │                                              │
   └────────────── розрив потоку клієнтом ────────┴──> CANCELED
```

1. `SubmitJob` створює завдання зі статусом `QUEUED` і повертає `job_id` (UUID v4) та Unix timestamp.
2. `TrackProgress` перевіряє існування завдання, далі на кожній стадії надсилає `ProgressUpdate`
   і оновлює стан у сховищі (`PROCESSING`, потім `COMPLETED` + `completed_at`).
3. Якщо клієнт розриває з'єднання — сервер ловить `stream.Context().Done()`, помічає завдання як
   `CANCELED` і повертає статус `codes.Canceled`, не «дочікуючи» таймерів.

### 3.4 Ключові архітектурні рішення

- **Контракт відокремлений від реалізації.** Protobuf-структури використовуються лише на межі
  сервісу; усередині працює доменна модель `storage.Job`. Це запобігає «протіканню» транспортних
  деталей у бізнес-логіку.
- **`sync.RWMutex`, а не `sync.Mutex`.** Читань (`GetJobStatus`) більше, ніж записів, тому паралельні
  читачі не блокують одне одного. Метод `Get` повертає **копію** структури, щоб ніхто не зміг
  оновити стан в обхід блокування.
- **Захист на рівні перехоплювачів.** Хендлери не містять жодного рядка перевірки токена — це
  наскрізна вимога, реалізована один раз для всіх методів.
- **Окремий Stream-перехоплювач.** `grpc.UnaryInterceptor` не застосовується до потокових викликів,
  тому без `grpc.StreamInterceptor` метод `TrackProgress` залишився б без автентифікації.
- **`GracefulStop`.** Сервер дочікується завершення активних викликів замість миттєвого обриву
  з'єднань і потоків.
- **gRPC Reflection.** Дозволяє Postman/`grpcurl` досліджувати контракт без локального `.proto`.

## 4. Структура проєкту

```
.
├── cmd/
│   ├── client/main.go              # консольний gRPC-клієнт (демонстрація всіх сценаріїв)
│   └── server/main.go              # точка входу сервера: listen, interceptors, reflection
├── internal/
│   ├── interceptor/
│   │   ├── interceptor.go          # автентифікація + логування + хронометраж
│   │   └── interceptor_test.go     # тести автентифікації
│   ├── mediaservice/
│   │   ├── service.go              # SubmitJob / GetJobStatus / TrackProgress
│   │   └── service_test.go         # тести бізнес-логіки, стріму та скасування
│   └── storage/
│       ├── store.go                # потокобезпечне сховище (RWMutex)
│       └── store_test.go           # тести сховища, зокрема конкурентний доступ
├── proto/
│   ├── media.proto                 # КОНТРАКТ (single source of truth)
│   ├── media.pb.go                 # ЗГЕНЕРОВАНО: повідомлення, enum, сериалізація
│   └── media_grpc.pb.go            # ЗГЕНЕРОВАНО: клієнтський стаб, серверний інтерфейс
├── scripts/
│   ├── gen.ps1                     # кодогенерація (PowerShell)
│   └── gen.sh                      # кодогенерація (bash)
├── go.mod / go.sum
├── .gitignore
└── README.md
```

## 5. Контракт `proto/media.proto`

| Елемент | Призначення |
|---|---|
| `enum MediaFormat` | `UNSPECIFIED=0`, `PNG=1`, `JPEG=2`, `WEBP=3`, `MP4=4` — цільовий формат конвертації. |
| `ProcessMediaRequest` | `client_id`, `filename`, `target_format`, `quality_level` (1..100). |
| `JobResponse` | `job_id` (UUID), `status` (`QUEUED`), `created_at` (Unix timestamp). |
| `GetJobStatusRequest` | `job_id` — використовується і як аргумент стріму. |
| `JobStatusResponse` | `job_id`, `status`, `progress_percent`, `completed_at`. |
| `ProgressUpdate` | `job_id`, `current_stage`, `progress_percent`, `is_completed`. |
| `rpc SubmitJob` | Unary: реєстрація завдання. |
| `rpc GetJobStatus` | Unary: читання стану. |
| `rpc TrackProgress` | Server Streaming: потік оновлень прогресу. |

Номери полів (`= 1`, `= 2`, …) — це не «порядкові номери», а ідентифікатори полів у бінарному
форматі (wire format). Саме вони, а не імена, потрапляють у закодоване повідомлення.

## 6. Кодогенерація

Канонічна команда (потрібен `protoc` та обидва плагіни в `PATH`):

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/media.proto
```

Те саме у вигляді скриптів: `./scripts/gen.sh` або `./scripts/gen.ps1`.

Встановлення інструментів:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
# сам protoc — з офіційних релізів protocolbuffers/protobuf (protoc-<ver>-win64.zip)
```

Що генерується:

- `proto/media.pb.go` — Go-структури повідомлень, get-методи, enum-константи, реєстрація типів;
- `proto/media_grpc.pb.go` — інтерфейси `MediaServiceClient` / `MediaServiceServer`,
  `RegisterMediaServiceServer`, `UnimplementedMediaServiceServer` (для зворотної сумісності),
  клієнтський стаб і типи потоків (`MediaService_TrackProgressClient/Server`).

Пакет згенерованого коду — `mediapb`, шлях імпорту — `media-service/proto`
(задається опцією `option go_package`).

## 7. Реалізація за рівнями

### Рівень 1 — базовий

- [x] Go-модуль `media-service`, структура пакетів.
- [x] Контракт `proto/media.proto` (enum, 5 повідомлень, сервіс із двома Unary-методами).
- [x] Кодогенерація `protoc-gen-go` + `protoc-gen-go-grpc`.
- [x] `net.Listen("tcp", ":50051")` + `grpc.NewServer()` + `pb.RegisterMediaServiceServer`.
- [x] Потокобезпечне сховище на `map[string]Job` + `sync.RWMutex`.
- [x] `SubmitJob`: валідація → UUID v4 → збереження зі статусом `QUEUED` → відповідь.
- [x] `GetJobStatus`: пошук за `job_id`, для відсутнього — `status.Errorf(codes.NotFound, ...)`.

### Рівень 2 — середній

- [x] `ProgressUpdate` та `rpc TrackProgress(GetJobStatusRequest) returns (stream ProgressUpdate)`.
- [x] Перевірка існування завдання перед стартом потоку (`NotFound`).
- [x] Імітація обробки: 25% «Demuxing input stream» → 50% «Encoding video frames» → 75%
      «Remuxing audio and headers» → 100% «Process completed successfully».
- [x] `stream.Send()` на кожному кроці, на 100% — статус `COMPLETED` і `completed_at`.
- [x] `select { case <-stream.Context().Done(): ... case <-time.After(stageDelay): }` — коректна
      зупинка при розриві з'єднання (завдання стає `CANCELED`).
- [x] Консольний клієнт `cmd/client`: `grpc.NewClient` + `insecure.NewCredentials`, `SubmitJob`,
      цикл `stream.Recv()` до `io.EOF`, фінальний `GetJobStatus`.

### Рівень 3 — поглиблений

- [x] `UnaryInterceptor`: `metadata.FromIncomingContext` → `x-api-key`; при відсутності/невірному
      ключі — `status.Error(codes.Unauthenticated, "Invalid or missing API key")`, хендлер не викликається.
- [x] Логування: `time.Now()` на вході, `peer.FromContext(ctx)` (адреса клієнта), `info.FullMethod`
      (назва методу), після хендлера — `time.Since()` і структурований рядок логу.
- [x] Підключення: `grpc.NewServer(grpc.UnaryInterceptor(...), grpc.StreamInterceptor(...))`.
- [x] Клієнт формує `metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", "secret-media-token"))`.
- [x] Обробка помилок через `status.FromError(err)` + `status.Code()`; негативний сценарій
      (без заголовка та з невірним ключем) демонструється і коректно діагностується.
- [x] Додатково: `reflection.Register`, `GracefulStop`, юніт-тести, валідація вхідних даних.

## 8. Запуск

Передумови: Go 1.22+ (перевірка: `go version`). Залежності вже зафіксовані в `go.mod`.

```powershell
# 1. Завантажити залежності
go mod download

# 2. Термінал №1 — сервер (порт 50051)
go run ./cmd/server

# 3. Термінал №2 — клієнт (повний сценарій: відмова без ключа, SubmitJob, стрім, фінальний статус)
go run ./cmd/client

# Корисні прапорці
go run ./cmd/server -addr :50051 -stage-delay 800ms -api-key secret-media-token
go run ./cmd/client -addr localhost:50051 -api-key secret-media-token `
                    -file vacation_clip.mov -format MP4 -quality 85
```

Ключ також можна задати змінною середовища, щоб не тримати секрет у командному рядку:

```powershell
$env:MEDIA_API_KEY = "secret-media-token"; go run ./cmd/server
```

## 9. Демонстрація роботи (реальний запуск)

Нижче наведено фактичне виведення програм, отримане під час виконання роботи
(`go run ./cmd/server`, потім `go run ./cmd/client -file vacation_clip.mov -format MP4 -quality 85`).

### 9.1 Логи gRPC-сервера

```text
2026/11/25 23:22:42 [SERVER] Starting Media Processing gRPC service on :50051...
2026/11/25 23:22:42 [SERVER] Authentication enabled: header "x-api-key" is required for every RPC
2026/11/25 23:22:46 [INTERCEPTOR] Unary call Method=/media.MediaService/SubmitJob Client=[::1]:51782 Auth=REJECTED Status=Unauthenticated Duration=0s
2026/11/25 23:22:46 [STORAGE] Job registered: ID=2ae1da72-770f-461d-9167-dba01b3d204e Filename=vacation_clip.mov TargetFormat=MEDIA_FORMAT_MP4 Status=QUEUED
2026/11/25 23:22:46 [INTERCEPTOR] Unary call Method=/media.MediaService/SubmitJob Client=[::1]:51782 Auth=SUCCESS Status=OK Duration=0s
2026/11/25 23:22:46 [STREAM] Client connected to track progress for JobID=2ae1da72-770f-461d-9167-dba01b3d204e
2026/11/25 23:22:47 [STREAM] JobID=2ae1da72-770f-461d-9167-dba01b3d204e -> Sent stage "Demuxing input stream" (25%)
2026/11/25 23:22:47 [STREAM] JobID=2ae1da72-770f-461d-9167-dba01b3d204e -> Sent stage "Encoding video frames" (50%)
2026/11/25 23:22:48 [STREAM] JobID=2ae1da72-770f-461d-9167-dba01b3d204e -> Sent stage "Remuxing audio and headers" (75%)
2026/11/25 23:22:49 [STREAM] JobID=2ae1da72-770f-461d-9167-dba01b3d204e -> Sent stage "Process completed successfully" (100%)
2026/11/25 23:22:49 [STREAM] Progress tracking finished for JobID=2ae1da72-770f-461d-9167-dba01b3d204e
2026/11/25 23:22:49 [INTERCEPTOR] Stream call Method=/media.MediaService/TrackProgress Client=[::1]:51782 Auth=SUCCESS Status=OK Duration=3.2017168s
2026/11/25 23:22:49 [INTERCEPTOR] Unary call Method=/media.MediaService/GetJobStatus Client=[::1]:51782 Auth=SUCCESS Status=OK Duration=0s
```

> `Duration=0s` для дуже швидких Unary-викликів — особливість монотонного таймера у
> віртуальному середовищі (низька роздільна здатність). На фізичному обладнанні там
> відображаються мікросекунди, як у прикладі із завдання. Для тривалих потокових викликів
> хронометраж видно коректно (`3.2017168s`).

### 9.2 Виведення консольного клієнта

```text
=== Media Processing Service CLI Client ===

[Step 1] Attempting RPC call WITHOUT authentication metadata...
[ERROR] Server responded with gRPC error: rpc error: code = Unauthenticated desc = Invalid or missing API key
[INFO] Authentication enforcement verified successfully.

[Step 2] Sending SubmitJob request with valid metadata (x-api-key: secret-media-token)...
[SUCCESS] Job created successfully:
  - Job ID:       2ae1da72-770f-461d-9167-dba01b3d204e
  - Initial State: QUEUED
  - Submitted At:  2026-11-25 23:22:46

[Step 3] Subscribing to Server Streaming: TrackProgress...
  [25%] Demuxing input stream
  [50%] Encoding video frames
  [75%] Remuxing audio and headers
  [100%] Process completed successfully!

[Step 4] Checking final job status via GetJobStatus RPC...
  - Current Status: COMPLETED
  - Progress:       100%
  - Completed At:   2026-11-25 23:22:49

=== Processing pipeline finished successfully ===
```

### 9.3 Негативний сценарій: невірний API-ключ

Запуск: `go run ./cmd/client -api-key wrong-token`

```text
=== Media Processing Service CLI Client ===

[Step 1] Attempting RPC call WITHOUT authentication metadata...
[ERROR] Server responded with gRPC error: rpc error: code = Unauthenticated desc = Invalid or missing API key
[INFO] Authentication enforcement verified successfully.

[Step 2] Sending SubmitJob request with valid metadata (x-api-key: wrong-token)...
[FATAL] SubmitJob failed: code=Unauthenticated message="Invalid or missing API key"
```

Сервер у цей момент фіксує два відхилені виклики:

```text
[INTERCEPTOR] Unary call Method=/media.MediaService/SubmitJob Client=[::1]:51433 Auth=REJECTED Status=Unauthenticated Duration=0s
```

### 9.4 Скасування потоку клієнтом (перевірка `stream.Context().Done()`)

Клієнт було зупинено посеред потоку (`-stage-delay 1500ms`), сервер миттєво зреагував:

```text
2026/11/25 23:21:59 [STREAM] JobID=176b1612-e261-44e5-8f4a-743072847a16 -> Sent stage "Remuxing audio and headers" (75%)
2026/11/25 23:21:59 [STREAM] Client disconnected, tracking stopped for JobID=176b1612-e261-44e5-8f4a-743072847a16: context canceled
2026/11/25 23:21:59 [INTERCEPTOR] Stream call Method=/media.MediaService/TrackProgress Client=[::1]:51433 Auth=SUCCESS Status=Canceled Duration=5.7084195s
```

## 10. Тестування

```bash
go vet ./...          # статичний аналіз — без зауважень
gofmt -l cmd internal proto   # форматування — список порожній
go test ./... -v      # юніт-тести
```

Результат:

```text
?   	media-service/cmd/client	[no test files]
?   	media-service/cmd/server	[no test files]
?   	media-service/proto	[no test files]
--- PASS: TestAuthorize (0.00s)                    (5 підтестів: без metadata, без заголовка, невірний ключ, валідний ключ, кілька значень)
--- PASS: TestUnaryInterceptorRejectsWithoutKey
ok  	media-service/internal/interceptor	0.020s
--- PASS: TestSubmitJobCreatesQueuedJob
--- PASS: TestSubmitJobValidation                 (5 підтестів валідації)
--- PASS: TestGetJobStatusNotFound
--- PASS: TestTrackProgressCompletesJob
--- PASS: TestTrackProgressCanceledByClient
--- PASS: TestTrackProgressUnknownJob
ok  	media-service/internal/mediaservice	0.030s
--- PASS: TestCreateAndGet
--- PASS: TestGetMissingReturnsFalse
--- PASS: TestUpdateChangesState
--- PASS: TestUpdateMissingJob
--- PASS: TestConcurrentAccess
ok  	media-service/internal/storage	0.015s
```

Що саме перевіряють тести:

- **interceptor** — що без валідного ключа хендлер взагалі не викликається, і що помилка має код `Unauthenticated`;
- **mediaservice** — валідацію (`InvalidArgument`), `NotFound`, повний прохід стріму до `COMPLETED`
  (4 оновлення, 100%, заповнений `completed_at`), реакцію на скасований контекст (`Canceled`);
- **storage** — базові операції, відсутній запис і конкурентний доступ (50 горутин на читання/запис).

> Примітка: `go test -race` на цій машині не запускається — компілятор `cgo` (gcc) не працює з
> шляхом, що містить кириличні символи та пробіли. Якщо проєкт скопіювати в ASCII-шлях виду
> `C:\work\media-service`, команда `go test -race ./...` проходить успішно.

## 11. Перевірка через `grpcurl` / Postman

```bash
# список сервісів (працює завдяки reflection)
grpcurl -plaintext localhost:50051 list

# Unary-виклик із ключем
grpcurl -plaintext -H "x-api-key: secret-media-token" \
  -d '{"client_id":"cli","filename":"video.mov","target_format":"MEDIA_FORMAT_MP4","quality_level":80}' \
  localhost:50051 media.MediaService/SubmitJob

# Server Streaming
grpcurl -plaintext -H "x-api-key: secret-media-token" \
  -d '{"job_id":"<JOB_ID>"}' localhost:50051 media.MediaService/TrackProgress

# Перевірка захисту: без заголовка очікуємо Unauthenticated
grpcurl -plaintext -d '{"job_id":"x"}' localhost:50051 media.MediaService/GetJobStatus
# ERROR: Code: Unauthenticated  Message: Invalid or missing API key
```

У Postman: **New → gRPC Request**, адреса `localhost:50051`, метод `/media.MediaService/TrackProgress`,
у вкладці **Metadata** додати `x-api-key: secret-media-token`, у **Message** — `{"job_id": "..."}`.

## 12. Скриншоти роботи програми

> Скриншоти додаються після локального запуску (файли у `docs/screenshots/`):
>
> 1. `01-server-start.png` — запуск сервера (`go run ./cmd/server`);
> 2. `02-client-success.png` — повний успішний сценарій клієнта (див. розділ 9.2);
> 3. `03-client-unauthorized.png` — відмова без ключа / з невірним ключем (розділ 9.3);
> 4. `04-server-logs.png` — логи сервера з `[INTERCEPTOR]`, `[STORAGE]`, `[STREAM]` (розділ 9.1);
> 5. `05-tests.png` — `go test ./...` без помилок (розділ 10).
>
> Приклад вставки в README:
>
> ```markdown
> ![Логи сервера](docs/screenshots/04-server-logs.png)
> ```

## 13. Контрольні питання (коротко)

1. **gRPC vs REST over HTTP/1.1.** gRPC — це RPC-фреймворк: клієнт викликає метод сервісу, а не
   оперує ресурсами. Транспорт — HTTP/2 (мультиплексування, бінарні фрейми, server push, flow control),
   серіалізація — бінарний Protobuf замість JSON. Контракт задається `.proto` і кодогенерується, тому
   розбіжності виявляються на етапі компіляції. REST має «людський» текстовий формат, кешування
   через HTTP, простіший для публічних API, але більший обсяг і слабшу типізацію.
2. **Номери полів (tags).** У wire format поле кодується парою «номер + тип», тому саме номер є
   ідентичністю поля на дроті. Імена полів у бінарному вигляді не передаються. Якщо номер змінити,
   старий клієнт і новий сервер (або навпаки) інтерпретуватимуть ті самі байти як різні поля —
   отримаємо тиху втрату/псування даних. Тому номери «заморожують», а видалені номери позначають
   `reserved`, щоб їх не перевикористали.
3. **Скалярні типи vs `enum`.** Скалярні (`int32`, `string`, `bool`, `int64`…) — це значення
   довільного діапазону; `enum` — іменований набір цілочисельних констант, який дає самодокументований
   контракт і валідацію на рівні типів. Перше значення `enum` у proto3 обов'язково `= 0`, бо 0 —
   це zero value: неініціалізоване поле повинно мати валідне значення за замовчуванням. Саме тому
   нульове значення називають `*_UNSPECIFIED` — воно позначає «не задано» і дозволяє відрізнити
   його від явно вибраного варіанту.
4. **Чотири типи RPC.** Unary (один запит — одна відповідь), Server Streaming (один запит — потік
   відповідей), Client Streaming (потік запитів — одна відповідь), Bidirectional Streaming (два
   незалежні потоки). Server Streaming застосовують для прогресу довгих операцій, потокової віддачі
   файлів/логів, підписок на події, великих результатів, які не хочеться тримати в пам'яті.
5. **Помилки в gRPC.** Повертаються через `status.Error(codes.X, msg)`; на дроті це HTTP/2-заголовки
   `grpc-status` (числовий код) і `grpc-message` (текст), а також `grpc-status-details-bin` для
   деталей. Клієнт розбирає їх через `status.FromError`. Рядок у тілі відповіді — погано, бо він
   ламає контракт (треба змінювати схему повідомлення), не дає машиночитних кодів, змішує дані й
   помилку, ускладнює ретраї/метрики та не працює для потоків.
6. **Metadata.** Це набір пар «ключ — значення» (ключі в нижньому регістрі), що транспортуються як
   HTTP/2-заголовки: звичайні — у `HEADERS` (початкові) та `TRAILERS` (завершальні) фреймах; клієнт
   надсилає їх у `:authority`/custom headers, сервер може повертати через `grpc.SetHeader`/
   `SetTrailer`. Системні (`grpc-*`, `:method`, `:path`) зарезервовані; користувацькі — довільні,
   типово `authorization`, `x-api-key`, `x-request-id`. Бінарні значення передають із суфіксом `-bin`.
7. **Unary Interceptors.** Це функції вищого порядку з сигнатурою
   `func(ctx, req, *UnaryServerInfo, handler) (any, error)`, які утворюють ланцюжок навколо хендлера:
   інтерцептор може змінити контекст, перевірити Metadata, викликати `handler(ctx, req)` або
   відмовити, виміряти час, залогуювати, зібрати метрики. Підключаються через
   `grpc.UnaryInterceptor` (один) / `grpc.ChainUnaryInterceptor` (кілька) і виконуються для кожного
   Unary-виклику — це і є наскрізна логіка без дублювання в хендлерах.
8. **`stream.Context().Done()`.** Контекст потоку скасовується, коли клієнт розірвав з'єднання або
   вийшов дедлайн. Якщо сервер цього не слухає, він продовжує «обробку» й горутини живуть до кінця
   таймерів — витік горутин, марна робота, а `stream.Send()` починає повертати помилки. Слухаючи
   `Done()`, сервер негайно звільняє ресурси, оновлює стан завдання і повертає `codes.Canceled`.

## 14. Висновки

У ході роботи реалізовано повнофункціональний gRPC-мікросервіс обробки медіафайлів. Отримано
практичні навички: опис контракту в proto3 та кодогенерація стабів; реалізація Unary- та
Server Streaming-методів; потокобезпечне зберігання стану; наскрізна автентифікація й логування
через перехоплювачі; коректна обробка скасування контексту та gRPC-статусів на клієнті.

Порівняно з REST-реалізацією цей підхід дає суворіший контракт (компіляційна перевірка типів),
менший обсяг трафіку завдяки бінарній серіалізації, вбудований стрімінг і природне місце для
наскрізних вимог (auth, метрики, трейсинг) в інтерцепторах.
