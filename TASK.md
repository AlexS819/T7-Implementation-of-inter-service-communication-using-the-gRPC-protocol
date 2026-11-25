# Практична робота № 7

## Тема: Реалізація міжсервісної взаємодії за протоколом gRPC

---

## Мета роботи

- Опанувати концепцію віддаленого виклику процедур (Remote Procedure Call, RPC) та архітектурні відмінності gRPC від класичних REST API.
- Засвоїти мову опису інтерфейсів Protocol Buffers (proto3) для створення суворих бінарних контрактів міжсервісної взаємодії.
- Опанувати інструментарій кодогенерації `protoc` із плагінами `protoc-gen-go` та `protoc-gen-go-grpc` для автоматичного створення структур і клієнтських стабів у Go.
- Навчитися реалізовувати серверні інтерфейси gRPC для обробки Unary-запитів та потокових відповідей (Server Streaming RPC).
- Опанувати розробку наскрізних перехоплювачів (Unary Interceptors) для автентифікації клієнтів за допомогою gRPC Metadata та вимірювання затримок обробки запитів.

---

## Технічне забезпечення

- Операційна система: Windows, macOS або Linux.
- Середовище розробки: Visual Studio Code (з офіційними розширеннями Go та vscode-proto3) або JetBrains GoLand.
- Компілятор: Go SDK версії 1.22 або новішої.
- Компілятор контрактів: утиліта `protoc` (Protocol Buffer Compiler) та плагіни `protoc-gen-go`, `protoc-gen-go-grpc`.
- Термінал / CLI: bash, zsh або PowerShell.
- Система контролю версій: Git.
- Інструменти тестування: Postman (із підтримкою gRPC-запитів), BloomRPC або консольна утиліта `grpcurl`.

---

## Короткі теоретичні відомості

### 1. Концепція gRPC та переваги над REST API

**gRPC (gRPC Remote Procedure Calls)** — це високопродуктивний міжсервісний RPC-фреймворк із відкритим вихідним кодом, створений компанією Google. На відміну від стилю REST, орієнтованого на ресурси та текстовий формат JSON, gRPC базується на виклику віддалених функцій з використанням бінарного протоколу серіалізації **Protocol Buffers** та транспортного протоколу **HTTP/2**.

Ключові переваги gRPC у розподілених системах:

- **Компактність та швидкодія:** бінарна серіалізація Protobuf у 3–10 разів компактніша за текстовий JSON і вимагає значно менше процесорного часу на маршалінг та демаршалінг.
- **Сувора типізація та строгий контракт:** файл `.proto` є єдиним джерелом правди (Single Source of Truth) для клієнта та сервера, що запобігає розбіжностям у форматах даних на етапі компіляції.
- **Мультиплексування HTTP/2:** передача багатьох паралельних запитів через єдине TCP-з'єднання без блокування черги (Head-of-Line Blocking).
- **Вбудована підтримка потоків (Streaming):** підтримка клієнтських, серверних та двонапрямлених потоків передачі даних.

---

### 2. Структура файлу контракту Protocol Buffers (proto3)

Контракт gRPC описується у файлі з розширенням `.proto`. Він визначає версію синтаксису, пакет, структури повідомлень із номерами полів (field tags) та сервісні RPC-методи:

```protobuf
syntax = "proto3";

package media;
option go_package = "media-service/proto;mediapb";

enum MediaFormat {
  MEDIA_FORMAT_UNSPECIFIED = 0;
  MEDIA_FORMAT_PNG = 1;
  MEDIA_FORMAT_JPEG = 2;
  MEDIA_FORMAT_WEBP = 3;
  MEDIA_FORMAT_MP4 = 4;
}

message ProcessMediaRequest {
  string client_id = 1;
  string filename = 2;
  MediaFormat target_format = 3;
  int32 quality_level = 4;
}

message JobResponse {
  string job_id = 1;
  string status = 2;
  int64 created_at = 3;
}

service MediaService {
  rpc SubmitJob(ProcessMediaRequest) returns (JobResponse);
}
```

Кодогенерація для мови Go здійснюється командою:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/media.proto
```

---

### 3. Серверна реалізація та потокова передача (Server Streaming)

Сервер gRPC створюється на базі мережевого слухача `net.Listener` і реєструє структуру, що впроваджує згенерований інтерфейс `MediaServiceServer`.

При Server Streaming RPC клієнт відправляє один запит, а сервер повертає потік повідомлень через виклик методу `stream.Send()`. Для запобігання витоку горутин сервер зобов'язаний відстежувати скасування клієнтського контексту `stream.Context().Done()`:

```go
func (s *server) TrackProgress(req *mediapb.GetJobStatusRequest, stream mediapb.MediaService_TrackProgressServer) error {
    for percent := 10; percent <= 100; percent += 30 {
        select {
        case <-stream.Context().Done():
            return status.Error(codes.Canceled, "Client canceled tracking")
        case <-time.After(300 * time.Millisecond):
            update := &mediapb.ProgressUpdate{
                JobId:           req.GetJobId(),
                ProgressPercent: int32(percent),
                IsCompleted:     percent == 100,
            }
            if err := stream.Send(update); err != nil {
                return err
            }
        }
    }
    return nil
}
```

---

### 4. Метадані (Metadata) та перехоплювачі (Interceptors)

gRPC дозволяє передавати контекстну інформацію (наприклад, токени автентифікації, ідентифікатори трасування) через **Metadata**, що передаються як HTTP/2-заголовки.

Для реалізації наскрізної логіки (автентифікація, логування, метрики) використовуються **Interceptors** (аналог middleware в HTTP-серверах):

```go
func AuthUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return nil, status.Error(codes.Unauthenticated, "Metadata is missing")
    }

    tokens := md.Get("x-api-key")
    if len(tokens) == 0 || tokens[0] != "secret-token" {
        return nil, status.Error(codes.Unauthenticated, "Invalid or missing API key")
    }

    return handler(ctx, req)
}
```

---

## Постановка задачі

У сучасних розподілених системах обробка «важких» обчислювальних операцій (масштабування графіки, транскодування медіа або конвертація форматів) виноситься в окремі автономні мікросервіси.

Необхідно спроєктувати та реалізувати повнофункціональний внутрішній мікросервіс обробки та конвертації медіафайлів (**Media Processing Service**), взаємодія з яким відбувається виключно за протоколом gRPC. Сервіс повинен надавати інтерфейс для реєстрації завдань, опитування поточного статусу, реального потокового інформування клієнта про прогрес операції через Server Streaming RPC, а також захищати API за допомогою перевірки сервісних токенів у метаданих запиту. 

Мережева взаємодія клієнт-сервер, серіалізація даних через Protobuf та механізми перехоплення є реальними та функціональними, тоді як ресурсомісткий процес обробки медіафайлу імітується покроковою зміною станів за таймером у фоні.

---

## Завдання

Практична робота складається з трьох послідовних рівнів. Кожен наступний рівень базується на попередньому та розширює функціональність розроблюваного рішення.

### Рівень 1. Базовий

1. Створити робочу директорію проєкту та ініціалізувати Go-модуль:
   ```bash
   go mod init media-service
   ```
2. Спроєктувати файл контракту `proto/media.proto` із синтаксисом `proto3`:
   - Оголосити перелічуваний тип `MediaFormat` із підтримуваними форматами: `MEDIA_FORMAT_UNSPECIFIED = 0`, `MEDIA_FORMAT_PNG = 1`, `MEDIA_FORMAT_JPEG = 2`, `MEDIA_FORMAT_WEBP = 3`, `MEDIA_FORMAT_MP4 = 4`.
   - Описати структуру запиту на обробку `ProcessMediaRequest`:
     - `client_id` (string) — унікальний ідентифікатор сервісу-клієнта;
     - `filename` (string) — ім'я вхідного файлу;
     - `target_format` (MediaFormat) — цільовий формат файлу;
     - `quality_level` (int32) — бажаний рівень компресії або якості (від 1 до 100).
   - Описати структуру відповіді `JobResponse`:
     - `job_id` (string) — згенерований унікальний UUID завдання;
     - `status` (string) — початковий статус завдання (наприклад, `QUEUED`);
     - `created_at` (int64) — Unix timestamp реєстрації завдання.
   - Описати повідомлення перевірки статусу `GetJobStatusRequest` (`job_id`) та відповіді `JobStatusResponse` (`job_id`, `status`, `progress_percent`, `completed_at`).
   - Описати сервіс `MediaService` із двома Unary RPC методами:
     - `rpc SubmitJob(ProcessMediaRequest) returns (JobResponse);`
     - `rpc GetJobStatus(GetJobStatusRequest) returns (JobStatusResponse);`
3. Виконати компіляцію контракту у Go-код за допомогою компілятора `protoc` із плагінами `protoc-gen-go` та `protoc-gen-go-grpc`.
4. Реалізувати сервер gRPC у пакеті `cmd/server/main.go`:
   - Відкрити мережевий TCP-порт `:50051` за допомогою `net.Listen()`.
   - Описати структуру сервера, яка впроваджує згенерований інтерфейс `pb.UnimplementedMediaServiceServer`.
   - Організувати потокобезпечне збереження зареєстрованих завдань у пам'яті сервера із застосуванням `sync.RWMutex`.
   - У методі `SubmitJob` генерувати новий UUID, зберігати початковий стан задачі зі статусом `QUEUED` та повертати клієнту сформовану відповідь.
   - У методі `GetJobStatus` здійснювати пошук завдання за його `job_id`. Якщо завдання відсутнє в пам'яті — повертати стандартну gRPC-помилку з кодом `codes.NotFound` через функцію `status.Errorf()`.
5. Запустити сервер та перевірити роботу обох Unary-методів за допомогою Postman (розділ gRPC) або консольної утиліти `grpcurl`.

### Рівень 2. Середній

1. Розширити контракт `proto/media.proto` підтримкою потокової передачі даних від сервера (Server Streaming):
   - Описати повідомлення `ProgressUpdate`:
     - `job_id` (string) — ідентифікатор завдання;
     - `current_stage` (string) — назва поточної технологічної стадії;
     - `progress_percent` (int32) — поточний прогрес у відсотках (від 0 до 100);
     - `is_completed` (bool) — ознака завершення процедури.
   - Додати до сервісу `MediaService` потоковий метод:
     ```protobuf
     rpc TrackProgress(GetJobStatusRequest) returns (stream ProgressUpdate);
     ```
   - Повторно запустити генерацію коду через `protoc`.
2. Реалізувати потоковий метод `TrackProgress` на сервері:
   - Перевірити існування вказаного завдання `job_id` у сховищі сервера.
   - Використовуючи цикл і таймер `time.After` (або `time.NewTicker`), імітувати покроковий процес обробки (наприклад: 25% — «Зчитування заголовків», 50% — «Транскодування медіапотоку», 75% — «Оптимізація розміру», 100% — «Обробка завершена»).
   - На кожному кроці надсилати оновлення клієнту через `stream.Send()`.
   - При досягненні 100% оновлювати статус завдання в пам'яті на `COMPLETED`.
   - Реалізувати слухача каналу скасування `stream.Context().Done()` для негайної зупинки фонової обробки у разі розриву з'єднання з клієнтом.
3. Розробити власний консольний gRPC-клієнт на Go у пакеті `cmd/client/main.go`:
   - Ініціалізувати з'єднання з сервером за допомогою функції `grpc.NewClient()` із використанням захисних параметрів `insecure.NewCredentials()`.
   - Створити клієнтський стаб через `pb.NewMediaServiceClient(conn)`.
   - Послідовно викликати метод `SubmitJob` для створення задачі та вивести отриманий `job_id`.
   - Викликати потоковий метод `TrackProgress` та організувати читання повідомлень у циклі `stream.Recv()`.
   - При отриманні помилки `io.EOF` коректно завершувати читання потоку та викликати `GetJobStatus` для виведення фінального стану об'єкта.

### Рівень 3. Поглиблений

1. Реалізувати серверний перехоплювач Unary Interceptor:
   - **Автентифікація через gRPC Metadata:** перевіряти наявність вхідних метаданих за допомогою `metadata.FromIncomingContext(ctx)`. Перевіряти заголовок `x-api-key`. Якщо заголовок відсутній або ключ не збігається з конфігураційним сервісним значенням (наприклад, `secret-media-token`) — негайно переривати запит та повертати статус помилки `status.Error(codes.Unauthenticated, "Invalid or missing API key")`.
   - **Логування та хронометраж виконання:** фіксувати точний час початку обробки виклику (`time.Now()`), мережеву адресу клієнта (`peer.FromContext(ctx)`) та назву викликаного методу (`info.FullMethod`). Після виконання хендлера розраховувати тривалість обробки за допомогою `time.Since()` та виводити структурований лог у консоль сервера.
   - Підключити інтерцептор до сервера під час ініціалізації: `grpc.NewServer(grpc.UnaryInterceptor(serverInterceptor))`.
2. Модифікувати Go CLI клієнт для взаємодії із захищеним сервером:
   - Сформувати контекст із метаданими за допомогою `metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", "secret-media-token"))`.
   - Реалізувати обробку помилок віддаленого сервера за допомогою `status.FromError(err)` та перевірки кодів результату через `status.Code()`.
   - Продемонструвати негативний сценарій: здійснити виклик без заголовка автентифікації або з некоректним ключем, перехопити статус `codes.Unauthenticated` і коректно сповістити користувача про відмову в доступі.

---

## Приклад очікуваного виведення програми

### Логи роботи gRPC-сервера (`cmd/server/main.go`):

```text
2026/09/08 17:55:00 [SERVER] Starting Media Processing gRPC service on :50051...
2026/09/08 17:55:10 [INTERCEPTOR] Unary call Method=/media.MediaService/SubmitJob Client=127.0.0.1:54320 Auth=REJECTED Status=Unauthenticated Duration=45.2µs
2026/09/08 17:55:15 [INTERCEPTOR] Unary call Method=/media.MediaService/SubmitJob Client=127.0.0.1:54322 Auth=SUCCESS Status=OK Duration=210.8µs
2026/09/08 17:55:15 [STORAGE] Job registered: ID=9b3c4a12-8871-4a8e-b3f9-7104b2c129e1 Filename=input_video.mov TargetFormat=MEDIA_FORMAT_MP4 Status=QUEUED
2026/09/08 17:55:16 [STREAM] Client connected to track progress for JobID=9b3c4a12-8871-4a8e-b3f9-7104b2c129e1
2026/09/08 17:55:16 [STREAM] JobID=9b3c4a12-8871-4a8e-b3f9-7104b2c129e1 -> Sent stage "Demuxing input stream" (25%)
2026/09/08 17:55:17 [STREAM] JobID=9b3c4a12-8871-4a8e-b3f9-7104b2c129e1 -> Sent stage "Encoding video frames" (50%)
2026/09/08 17:55:17 [STREAM] JobID=9b3c4a12-8871-4a8e-b3f9-7104b2c129e1 -> Sent stage "Remuxing audio and headers" (75%)
2026/09/08 17:55:18 [STREAM] JobID=9b3c4a12-8871-4a8e-b3f9-7104b2c129e1 -> Sent stage "Process completed successfully" (100%)
2026/09/08 17:55:18 [STREAM] Progress tracking finished for JobID=9b3c4a12-8871-4a8e-b3f9-7104b2c129e1
```

### Виведення консольного Go-клієнта (`cmd/client/main.go`):

```text
=== Media Processing Service CLI Client ===

[Step 1] Attempting RPC call WITHOUT authentication metadata...
[ERROR] Server responded with gRPC error: rpc error: code = Unauthenticated desc = Invalid or missing API key
[INFO] Authentication enforcement verified successfully.

[Step 2] Sending SubmitJob request with valid metadata (x-api-key: secret-media-token)...
[SUCCESS] Job created successfully:
  - Job ID:       9b3c4a12-8871-4a8e-b3f9-7104b2c129e1
  - Initial State: QUEUED
  - Submitted At:  2026-09-08 17:55:15

[Step 3] Subscribing to Server Streaming: TrackProgress...
  [25%] Demuxing input stream...
  [50%] Encoding video frames...
  [75%] Remuxing audio and headers...
  [100%] Process completed successfully!

[Step 4] Checking final job status via GetJobStatus RPC...
  - Current Status: COMPLETED
  - Progress:       100%
  - Completed At:   2026-09-08 17:55:18

=== Processing pipeline finished successfully ===
```

---

## Контрольні питання

1. У чому полягають ключові відмінності архітектури gRPC від класичного підходу REST over HTTP/1.1?
2. Яку роль відіграють номери полів (Field Numbers / Tags) у файлах визначення Protocol Buffers? Чому їх не можна змінювати після випуску сервісу в експлуатацію?
3. Чим відрізняються скалярні типи даних Protobuf від перелічуваних типів `enum`? Чому перше значення будь-якого `enum` у proto3 обов'язково повинно мати індекс `0`?
4. Які існують чотири типи RPC-комунікації у gRPC? У яких практичних сценаріях застосовується тип Server Streaming?
5. Яким чином gRPC-сервер організовує повернення помилок клієнту? Чому не рекомендується повертати помилки у вигляді звичайних рядків у тілі відповіді?
6. Що таке gRPC Metadata і які заголовки HTTP/2 використовуються для їх транспортування?
7. Як влаштований механізм Unary Interceptors у Go і як він дозволяє реалізувати наскрізні вимоги (автентифікація, логування, метрики)?
8. Чому важливо відстежувати стан каналу `stream.Context().Done()` під час виконання потокових RPC-методів на сервері?

---

## Критерії оцінювання

Оцінювання здійснюється за шкалою повноти реалізації рівнів практичної роботи:

- **«Задовільно» (Рівень 1):** реалізовано базовий прототип, створено структуру проєкту, програма успішно компілюється та виконує базові сценарії.
- **«Добре» (Рівень 2):** виконано базовий рівень та реалізовано ключову функціональність заняття, забезпечено надійність і безпеку виконання коду.
- **«Відмінно» (Рівень 3):** повністю завершено всі три рівні завдання з дотриманням кращих інженерних практик, чистоти архітектури та оптимізації.
- **Усний захист:** обов'язкове пояснення логіки коду та впевнені відповіді на контрольні питання викладача.

---

## Вимоги до звітності

1. Опублікувати вихідний код у Git-репозиторії.
2. Оформити файл `README.md` з описом проєкту, інструкцією запуску та обов'язковими скриншотами роботи програми (включаючи демонстрацію відсутності помилок).
3. Продемонструвати роботу програми викладачеві та захистити результати.
