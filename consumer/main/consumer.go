// main consumer.go
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	// firebase "firebase.google.com/go/v4" // Необходимо добавить импорт Firebase SDK
	// "google.golang.org/api/option" // Для аутентификации Firebase
)

// QueueMessage структура для разбора сообщения из очереди
type QueueMessage struct {
	UserID           string `json:"user_id"`
	SolutionID       string `json:"solution_id"`
	TaskID           string `json:"task_id,omitempty"`
	S3FileKey        string `json:"s3_file_key"`
	OriginalFilename string `json:"original_filename,omitempty"`
	Timestamp        int64  `json:"timestamp"`
}

// permanentError помечает ошибку, которую бессмысленно повторять:
// повторная обработка того же сообщения завершится точно так же
// (битый JSON, повреждённый архив, падение расчёта на тех же входных данных).
// Всё, что не помечено, считается временным (сеть, DNS, S3, диск) и уходит в очередь повторно.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// permanent оборачивает ошибку как неповторяемую
func permanent(format string, args ...interface{}) error {
	return &permanentError{err: fmt.Errorf(format, args...)}
}

// isPermanent сообщает, нужно ли отбросить сообщение вместо повторной постановки в очередь
func isPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}

// retryBackoff задаёт паузы между повторами временных операций.
// Суммарно покрывает ~105 секунд недоступности сети/DNS —
// заметно больше, чем внутренние ретраи AWS SDK (те укладываются в ~15 секунд).
var retryBackoff = []time.Duration{15 * time.Second, 30 * time.Second, 60 * time.Second}

// retryTransient повторяет операцию с паузами при ошибке
func retryTransient(opName string, fn func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt >= len(retryBackoff) {
			return fmt.Errorf("%s failed after %d attempts: %w", opName, attempt+1, err)
		}
		pause := retryBackoff[attempt]
		log.Printf("[RETRY] %s failed (attempt %d/%d): %v. Retrying in %s",
			opName, attempt+1, len(retryBackoff)+1, err, pause)
		time.Sleep(pause)
	}
}

// --- Функции для работы с Firebase (примерная структура) ---
// func sendLinkToFirebase(userID, solutionID, vtrFileKey string) error {
//     // 1. Инициализировать Firebase Admin SDK (один раз при запуске)
//     // 2. Сформировать публичный URL для vtrFileKey (если бакет публичный) или presigned URL
//     // 3. Записать URL в Firestore в коллекцию пользователя/решения
//     // Например: users/{userID}/solutions/{solutionID}/results/{resultID}
//     // doc.Set(map[string]interface{}{"download_url": publicURL, "timestamp": time.Now()})
//
//     // Заглушка
//     log.Printf("[FIREBASE] Would send link for user: %s, solution: %s, vtr_key: %s", userID, solutionID, vtrFileKey)
//     return nil // или возвращать ошибку
// }

func initS3Client() (*s3.Client, error) {
	// Загружаем .env файл (если есть)
	if err := godotenv.Load(); err != nil {
		log.Println("INFO: No .env file found in current directory. Using system environment variables.")
	} else {
		log.Println("INFO: .env file loaded successfully.")
	}

	// Читаем переменные окружения
	endpoint := os.Getenv("AWS_S3_ENDPOINT")
	region := os.Getenv("AWS_S3_REGION")
	bucketName := os.Getenv("AWS_S3_BUCKET_NAME")
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if endpoint == "" || region == "" || bucketName == "" || accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("one or more S3 environment variables are not set (endpoint, region, bucket_name, access_key_id, secret_access_key)")
	}

	// Создаем провайдер учетных данных
	credsProvider := credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")

	// Загружаем конфигурацию с увеличенными таймаутами
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credsProvider),
		config.WithRetryMaxAttempts(5), // Увеличиваем количество попыток с 3 до 5
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Создаем S3-клиент с кастомным эндпоинтом
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = region
		o.BaseEndpoint = aws.String(endpoint)
		// o.UsePathStyle = true // Включите, если S3-совместимое хранилище требует path-style
	})

	return s3Client, nil
}

func downloadFileFromS3(s3Client *s3.Client, bucketName, s3Key, localFilePath string) error {
	log.Printf("Downloading S3 object: %s from bucket: %s", s3Key, bucketName)

	// Создаем директорию, если не существует
	dir := filepath.Dir(localFilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create download directory: %w", err)
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
	}

	// Таймаут на всю операцию, включая чтение тела ответа:
	// без него зависшее соединение блокирует обработчик бесконечно
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	result, err := s3Client.GetObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Создаем локальный файл для записи
	outFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer outFile.Close()

	// Копируем содержимое из S3 в локальный файл
	_, err = io.Copy(outFile, result.Body)
	if err != nil {
		return fmt.Errorf("failed to copy S3 object content to local file: %w", err)
	}

	log.Printf("Successfully downloaded S3 object: %s to local file: %s", s3Key, localFilePath)
	return nil
}

// uploadFileToS3 загружает локальный файл в S3
func uploadFileToS3(s3Client *s3.Client, bucketName, localFilePath, s3Key string) error {
	log.Printf("Uploading local file: %s to S3 key: %s in bucket: %s", localFilePath, s3Key, bucketName)

	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to open local file for upload: %w", err)
	}
	defer file.Close()

	// Получаем размер файла для логирования s
	fileInfo, err := file.Stat()
	if err == nil {
		log.Printf("[UPLOAD] File size: %.2f MB", float64(fileInfo.Size())/(1024*1024))
	}

	// Создаем контекст с таймаутом 10 минут для больших файлов
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	putObjectInput := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Body:   file,
		// ACL:  types.ObjectCannedACL("private"), // Установите ACL по необходимости
	}

	log.Printf("[UPLOAD] Starting upload to S3...")
	_, err = s3Client.PutObject(ctx, putObjectInput)
	if err != nil {
		return fmt.Errorf("failed to upload file to S3: %w", err)
	}

	log.Printf("Successfully uploaded local file: %s to S3 key: %s", localFilePath, s3Key)
	return nil
}

// call_solve запускает внешнюю программу AP3DMT
func call_solve() error {
	log.Println("Starting AP3DMT application...")

	mcrPath := os.Getenv("MCR_PATH")
	if mcrPath == "" {
		mcrPath = "/opt/matlab" // путь внутри контейнера
	}
	appDir := os.Getenv("APP_DIR")
	if appDir == "" {
		appDir = "/app/AP3DMT_app/application"
	}

	// Диагностика: проверка окружения перед запуском
	log.Printf("[DIAGNOSTICS] MCR_PATH: %s", mcrPath)
	log.Printf("[DIAGNOSTICS] APP_DIR: %s", appDir)
	log.Printf("[DIAGNOSTICS] LD_LIBRARY_PATH: %s", os.Getenv("LD_LIBRARY_PATH"))
	log.Printf("[DIAGNOSTICS] MCR_CACHE_ROOT: %s", os.Getenv("MCR_CACHE_ROOT"))

	// Проверка существования MCR
	if _, err := os.Stat(mcrPath); os.IsNotExist(err) {
		return fmt.Errorf("MCR_PATH does not exist: %s", mcrPath)
	}
	log.Printf("[DIAGNOSTICS] MCR directory exists: %s", mcrPath)

	// Проверка существования рабочей директории
	if _, err := os.Stat(appDir); os.IsNotExist(err) {
		return fmt.Errorf("APP_DIR does not exist: %s", appDir)
	}
	log.Printf("[DIAGNOSTICS] APP_DIR exists: %s", appDir)

	// Проверка существования скрипта запуска
	scriptPath := filepath.Join(appDir, "run_AP3DMT_comp.sh")
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		return fmt.Errorf("run_AP3DMT_comp.sh not found at: %s", scriptPath)
	}
	log.Printf("[DIAGNOSTICS] Script found: %s", scriptPath)

	// Проверка прав на выполнение
	info, err := os.Stat(scriptPath)
	if err == nil {
		log.Printf("[DIAGNOSTICS] Script permissions: %s", info.Mode())
	}

	// Список файлов в APP_DIR для отладки
	files, err := os.ReadDir(appDir)
	if err == nil {
		log.Printf("[DIAGNOSTICS] Files in APP_DIR (%d total):", len(files))
		for i, f := range files {
			if i < 20 { // ограничиваем вывод первыми 20 файлами
				log.Printf("  - %s (dir: %v)", f.Name(), f.IsDir())
			}
		}
		if len(files) > 20 {
			log.Printf("  ... and %d more files", len(files)-20)
		}
	}

	// Проверка наличия входных данных (solution_data.zip или распакованных файлов)
	dataFiles := []string{"solution_data.zip", "AP3DMT.dat", "input.dat", "data.dat"}
	foundData := false
	for _, df := range dataFiles {
		if _, err := os.Stat(filepath.Join(appDir, df)); err == nil {
			log.Printf("[DIAGNOSTICS] Found data file: %s", df)
			foundData = true
		}
	}
	if !foundData {
		log.Printf("[WARNING] No expected data files found in APP_DIR")
	}

	cmd := exec.Command("./run_AP3DMT_comp.sh", mcrPath)
	cmd.Dir = appDir

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("[DIAGNOSTICS] Executing command: %s %s", cmd.Path, cmd.Args)
	log.Printf("[DIAGNOSTICS] Working directory: %s", cmd.Dir)

	err = cmd.Run()
	if err != nil {
		// Детальная информация об ошибке
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("[ERROR] AP3DMT exit code: %d", exitErr.ExitCode())
			log.Printf("[ERROR] AP3DMT exit error: %v", exitErr)
		}
		log.Printf("AP3DMT application finished with error: %v", err)
		return fmt.Errorf("AP3DMT execution failed: %w", err)
	}
	log.Println("AP3DMT application finished successfully.")
	return nil
}

// runPythonScript запускает Python-скрипт через системный Python
func runPythonScript() error {
	log.Printf("Starting Python script for conversion")

	appDir := os.Getenv("APP_DIR")
	if appDir == "" {
		appDir = "/app/AP3DMT_app/application"
	}

	// Прямой вызов системного Python3 (venv не нужен, зависимости установлены глобально в Dockerfile)
	cmd := exec.Command("python3", "method3.py")
	cmd.Dir = appDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("Python script finished with error: %v", err)
		return fmt.Errorf("python script execution failed: %w", err)
	}
	log.Printf("Python script finished successfully.")
	return nil
}

// unzipToFlatDir распаковывает ZIP-архив в целевую директорию "плоско"
// (все файлы извлекаются прямо в dest, игнорируя структуру папок внутри архива)
func unzipToFlatDir(src string, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %w", err)
	}
	defer r.Close()

	for _, f := range r.File {
		// Игнорируем директории внутри архива, берём только файлы
		if f.FileInfo().IsDir() {
			continue
		}

		// Извлекаем только имя файла (без путей), чтобы не создавать подпапки
		// Например, "folder/data.dat" -> "data.dat"
		fileName := filepath.Base(f.Name)
		if fileName == "" || fileName == "." || fileName == "/" {
			continue
		}

		fpath := filepath.Join(dest, fileName)

		// Открываем файл в архиве
		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("failed to open file in archive %s: %w", f.Name, err)
		}

		// Создаем файл на диске в целевой директории
		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			rc.Close()
			return fmt.Errorf("failed to create file %s: %w", fpath, err)
		}

		// Копируем содержимое
		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
		if err != nil {
			return fmt.Errorf("failed to copy file %s: %w", f.Name, err)
		}
		log.Printf("Extracted: %s -> %s", f.Name, fpath)
	}
	return nil
}

// --- Публикация статуса выполнения в S3 ---

// Этапы обработки решения. Значение попадает в поле stage файла status.json
// и предназначено для отображения на основном сервере.
const (
	stageValidating  = "validating"
	stageDownloading = "downloading"
	stageExtracting  = "extracting"
	stageComputing   = "computing"
	stageConverting  = "converting"
	stageUploading   = "uploading"
	stageCompleted   = "completed"
)

// stageOrder задаёт номер этапа для отрисовки прогресса на основном сервере.
// Терминальных этапов (completed/failed) в списке нет: у них номер равен общему
// числу этапов, то есть шкала заполнена.
var stageOrder = map[string]int{
	stageDownloading: 1,
	stageExtracting:  2,
	stageComputing:   3,
	stageConverting:  4,
	stageUploading:   5,
}

// stageTotal — общее число рабочих этапов, знаменатель шкалы прогресса
const stageTotal = 5

// Состояния решения в поле status файла status.json
const (
	statusRunning   = "running"
	statusCompleted = "completed"
	statusFailed    = "failed"
)

// heartbeatInterval — период перезаписи status.json во время длительных этапов.
// Расчёт AP3DMT идёт часами; без периодического обновления updated_at основной
// сервер не отличит идущий расчёт от упавшего потребителя.
const heartbeatInterval = 30 * time.Second

// statusAttemptTimeout ограничивает ОДНУ попытку публикации статуса:
// JSON крошечный, нормальный PUT проходит быстрее секунды. Короткий таймаут
// не даёт зависшему запросу надолго блокировать конвейер и быстрее уходит
// в повторную попытку.
const statusAttemptTimeout = 15 * time.Second

// Паузы между повторами публикации статуса.
// 409 OperationAborted ("A conflicting conditional operation is currently in
// progress") возникает, когда предыдущий PUT того же ключа ещё не завершился
// на стороне S3 (например, клиент оборвал его по таймауту, а сервер дописывает).
// Конфликт рассасывается сам за секунды-десятки секунд — его нужно просто переждать.
var (
	// Обычные этапы и пульс: 2 попытки. Пульс повторится через 30 с сам.
	statusRetryPauses = []time.Duration{3 * time.Second}
	// Терминальные статусы (completed/failed) — единственная точка, откуда
	// пользователь узнаёт результат: повторяем настойчиво, ~4 минуты суммарно.
	statusRetryPausesTerminal = []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 60 * time.Second}
)

// SolutionStatus — содержимое status.json, читаемое основным сервером
type SolutionStatus struct {
	UserID     string `json:"user_id"`
	SolutionID string `json:"solution_id"`
	TaskID     string `json:"task_id,omitempty"`
	Stage      string `json:"stage"`
	StageIndex int    `json:"stage_index"`
	StageTotal int    `json:"stage_total"`
	Status     string `json:"status"`
	Message    string `json:"message,omitempty"`
	Error      string `json:"error,omitempty"`
	// WillRetry сообщает основному серверу, ждать ли повторной попытки
	// или показывать пользователю окончательную ошибку
	WillRetry bool   `json:"will_retry"`
	ResultKey string `json:"result_key,omitempty"`
	StartedAt string `json:"started_at"`
	UpdatedAt string `json:"updated_at"`
}

// statusReporter публикует текущий этап обработки в S3.
// Все ошибки публикации только логируются: сбой отчёта о статусе не должен
// ронять сам расчёт.
type statusReporter struct {
	s3Client   *s3.Client
	bucketName string
	key        string
	startedAt  time.Time

	mu      sync.Mutex
	current SolutionStatus

	// stopHeartbeat останавливает фоновое обновление updated_at
	stopHeartbeat chan struct{}
	heartbeatDone chan struct{}
}

// statusKey возвращает ключ файла статуса рядом с результатами решения
func statusKey(userID, solutionID string) string {
	return fmt.Sprintf("users/%s/solutions/%s/status.json", userID, solutionID)
}

func newStatusReporter(s3Client *s3.Client, bucketName string, msg QueueMessage) *statusReporter {
	now := time.Now().UTC()
	return &statusReporter{
		s3Client:   s3Client,
		bucketName: bucketName,
		key:        statusKey(msg.UserID, msg.SolutionID),
		startedAt:  now,
		current: SolutionStatus{
			UserID:     msg.UserID,
			SolutionID: msg.SolutionID,
			TaskID:     msg.TaskID,
			// Начальный этап: если сообщение не пройдёт проверку полей,
			// в status.json попадёт stage=validating, а не пустая строка
			Stage:      stageValidating,
			StageTotal: stageTotal,
			Status:     statusRunning,
			StartedAt:  now.Format(time.RFC3339),
		},
	}
}

// publishOnce выполняет одну попытку PUT текущего статуса (payload готовится
// заново на каждую попытку, чтобы updated_at отражал время фактической записи)
func (r *statusReporter) publishOnce() error {
	r.current.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	payload, err := json.MarshalIndent(r.current, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal status: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), statusAttemptTimeout)
	defer cancel()

	_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(r.bucketName),
		Key:         aws.String(r.key),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/json"),
		// Статус читается опросом, поэтому запрещаем кэширование промежуточными узлами
		CacheControl: aws.String("no-cache"),
	})
	return err
}

// publish записывает текущее состояние в S3 с повторами (вызывается под
// удержанным mu). terminal=true — для completed/failed: эти статусы определяют,
// что увидит пользователь, поэтому повторяются настойчиво (409-конфликт ключа
// после оборванного PUT рассасывается сам за секунды-минуты).
// Ошибки публикации только логируются: сбой отчёта не должен ронять расчёт.
func (r *statusReporter) publish(terminal bool) {
	pauses := statusRetryPauses
	if terminal {
		pauses = statusRetryPausesTerminal
	}

	for attempt := 0; ; attempt++ {
		err := r.publishOnce()
		if err == nil {
			log.Printf("[STATUS] %s -> stage=%s status=%s", r.key, r.current.Stage, r.current.Status)
			return
		}
		if attempt >= len(pauses) {
			log.Printf("[STATUS] Failed to publish status to %s after %d attempts: %v", r.key, attempt+1, err)
			return
		}
		log.Printf("[STATUS] Publish to %s failed (attempt %d/%d): %v. Retrying in %s",
			r.key, attempt+1, len(pauses)+1, err, pauses[attempt])
		time.Sleep(pauses[attempt])
	}
}

// setStage публикует переход к новому этапу
func (r *statusReporter) setStage(stage, message string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.current.Stage = stage
	r.current.StageIndex = stageOrder[stage]
	r.current.Status = statusRunning
	r.current.Message = message
	r.current.Error = ""
	r.publish(false)
}

// complete публикует успешное завершение со ссылкой на результат
func (r *statusReporter) complete(resultKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.current.Stage = stageCompleted
	r.current.StageIndex = stageTotal
	r.current.Status = statusCompleted
	r.current.Message = "Расчёт завершён, результат загружен"
	r.current.Error = ""
	r.current.ResultKey = resultKey
	// Терминальный статус обязан дойти: без него пользователь не увидит
	// завершение, даже если all_models.zip уже в хранилище
	r.publish(true)
}

// fail публикует ошибку. willRetry сообщает, будет ли сообщение обработано повторно.
func (r *statusReporter) fail(err error, willRetry bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Этап (stage) не перезаписываем: по нему видно, на чём именно всё упало.
	// Терминальность отражена в поле status.
	r.current.Status = statusFailed
	r.current.Error = err.Error()
	r.current.WillRetry = willRetry
	if willRetry {
		r.current.Message = "Временный сбой, задача будет повторена"
	} else {
		r.current.Message = "Обработка завершилась ошибкой"
	}
	// Терминальный статус — повторяем настойчиво
	r.publish(true)
}

// startHeartbeat периодически перезаписывает status.json во время длительного этапа,
// обновляя updated_at. Основной сервер по устареванию updated_at определяет,
// что потребитель умер, не дойдя до терминального статуса.
func (r *statusReporter) startHeartbeat() {
	stop := make(chan struct{})
	done := make(chan struct{})
	r.stopHeartbeat = stop
	r.heartbeatDone = done

	go func() {
		defer close(done)
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				r.mu.Lock()
				// Терминальное состояние могло наступить между тиками —
				// не затираем completed/failed повторной публикацией running
				if r.current.Status == statusRunning {
					// Пульс не терминален: следующий тик через 30 с повторит сам
					r.publish(false)
				}
				r.mu.Unlock()
			}
		}
	}()
}

// stop останавливает фоновое обновление и дожидается завершения горутины
func (r *statusReporter) stop() {
	if r.stopHeartbeat == nil {
		return
	}
	close(r.stopHeartbeat)
	<-r.heartbeatDone
	r.stopHeartbeat = nil
}

// --- Идемпотентная обработка повторных доставок ---
//
// Если AMQP-соединение рвётся во время многочасового расчёта (network-сбой,
// consumer_timeout брокера, перезапуск RabbitMQ), неподтверждённое сообщение
// возвращается в очередь и доставляется повторно. Без проверок ниже это
// превращается в ещё один многочасовой расчёт той же задачи — сначала на
// исходном воркере (он не знает про разрыв), потом на подобравшем дубликат.

const (
	// период опроса status.json во время ожидания завершения чужого расчёта
	dedupePollInterval = 60 * time.Second
	// updated_at не обновлялся дольше — значит воркер умер, задачу перехватываем
	heartbeatStaleAfter = 5 * time.Minute
	// предел ожидания чужого воркера (расчёт идёт часами, но не вечно)
	dedupeMaxWait = 24 * time.Hour
)

// fetchStatus читает status.json решения; nil, если файла нет или он не читается
func fetchStatus(s3Client *s3.Client, bucketName string, msg QueueMessage) *SolutionStatus {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(statusKey(msg.UserID, msg.SolutionID)),
	})
	if err != nil {
		return nil
	}
	defer out.Body.Close()

	var st SolutionStatus
	if err := json.NewDecoder(out.Body).Decode(&st); err != nil {
		return nil
	}
	return &st
}

// resultExists проверяет наличие объекта результата в S3
func resultExists(s3Client *s3.Client, bucketName, key string) bool {
	if key == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	return err == nil
}

// defaultResultKey — путь результата, используемый processMessage,
// на случай если status.json записан без result_key
func defaultResultKey(msg QueueMessage) string {
	return fmt.Sprintf("users/%s/solutions/%s/results/all_models.zip", msg.UserID, msg.SolutionID)
}

// resultKeyOf достаёт ключ результата из статуса (с откатом на стандартный)
func resultKeyOf(st *SolutionStatus, msg QueueMessage) string {
	if st.ResultKey != "" {
		return st.ResultKey
	}
	return defaultResultKey(msg)
}

// resultUploadedAfter проверяет, что объект результата загружен ПОСЛЕ
// отправки этой задачи (LastModified > timestamp сообщения). Совпадение
// task_id в статусе + свежий результат означают, что расчёт фактически
// выполнен, даже если терминальный статус опубликовать не успели.
func resultUploadedAfter(s3Client *s3.Client, bucketName, key string, taskTimestamp int64) bool {
	if key == "" || taskTimestamp <= 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil || out.LastModified == nil {
		return false
	}
	return out.LastModified.Unix() > taskTimestamp
}

// repairCompletedStatus восстанавливает терминальный статус completed
// для задачи, чей результат уже лежит в S3 (воркер умер между загрузкой
// результата и публикацией статуса, либо статус не прошёл из-за 409).
func repairCompletedStatus(s3Client *s3.Client, bucketName string, msg QueueMessage, resultKey string) {
	r := newStatusReporter(s3Client, bucketName, msg)
	if msg.Timestamp > 0 {
		r.current.StartedAt = time.Unix(msg.Timestamp, 0).UTC().Format(time.RFC3339)
	}
	r.complete(resultKey) // публикация с настойчивыми повторами
}

// tryRepairFromResult — если результат загружен после отправки задачи,
// чинит статус completed и возвращает true (пересчёт не нужен).
func tryRepairFromResult(s3Client *s3.Client, bucketName string, msg QueueMessage) bool {
	key := defaultResultKey(msg)
	if !resultUploadedAfter(s3Client, bucketName, key, msg.Timestamp) {
		return false
	}
	log.Printf("[DEDUPE] Результат %s загружен после отправки задачи, но статус не завершён — восстанавливаю completed", key)
	repairCompletedStatus(s3Client, bucketName, msg, key)
	return true
}

// checkDuplicate проверяет status.json ПЕРЕД началом обработки:
//   - completed + результат на месте → задача уже решена, сообщение можно
//     подтвердить без пересчёта (типично: ack не дошёл после успешной обработки);
//   - running + свежий пульс → задачу считает другой воркер (или наша прошлая
//     доставка, ещё не завершившаяся) — ждём его финала вместо дубля;
//   - прочие состояния → считаем сами.
//
// Возвращает true, если расчёт не нужен и сообщение можно подтвердить.
func checkDuplicate(s3Client *s3.Client, bucketName string, msg QueueMessage) bool {
	if msg.TaskID == "" {
		return false // без task_id надёжно отличить дубликат невозможно
	}
	st := fetchStatus(s3Client, bucketName, msg)
	if st == nil || st.TaskID != msg.TaskID {
		return false // статуса нет или он от другой задачи
	}

	switch st.Status {
	case statusCompleted:
		key := resultKeyOf(st, msg)
		if resultExists(s3Client, bucketName, key) {
			log.Printf("[DEDUPE] Задача %s уже завершена (результат: %s) — подтверждаю без пересчёта", msg.TaskID, key)
			return true
		}
		log.Printf("[DEDUPE] Статус completed, но результат %s не найден — выполняю расчёт", key)
		return false

	case statusRunning:
		// Статус мог записать как другой воркер, так и наша предыдущая
		// доставка. Критерий различия — свежесть updated_at (пульс каждые 30 с).
		upd, err := time.Parse(time.RFC3339, st.UpdatedAt)
		if err != nil || time.Since(upd) > heartbeatStaleAfter {
			// Воркер умер — но, возможно, он успел загрузить результат
			// и не успел только опубликовать терминальный статус
			// (ровно этот сценорий: 409 на complete + смерть канала до ack).
			if tryRepairFromResult(s3Client, bucketName, msg) {
				return true
			}
			log.Printf("[DEDUPE] Статус running, но пульс устарел (updated_at=%s) — воркер умер, перехватываю задачу", st.UpdatedAt)
			return false
		}
		log.Printf("[DEDUPE] Задачу %s уже считает воркер (пульс %s назад) — жду завершения вместо дубля",
			msg.TaskID, time.Since(upd).Round(time.Second))
		return waitForOtherWorker(s3Client, bucketName, msg)
	}

	// failed и прочие: ошибка могла быть временной (сеть/S3) — пересчитываем
	return false
}

// waitForOtherWorker опрашивает status.json, пока чужой воркер жив (пульс свежий).
// Возвращает true, если задача завершилась успешно (сообщение можно подтвердить),
// false — если нужно взять расчёт на себя (воркер умер, упал или не уложился в лимит).
func waitForOtherWorker(s3Client *s3.Client, bucketName string, msg QueueMessage) bool {
	deadline := time.Now().Add(dedupeMaxWait)
	for time.Now().Before(deadline) {
		time.Sleep(dedupePollInterval)

		st := fetchStatus(s3Client, bucketName, msg)
		if st == nil || st.TaskID != msg.TaskID {
			log.Printf("[DEDUPE] status.json исчез/сменился во время ожидания — беру задачу на себя")
			return false
		}
		switch {
		case st.Status == statusCompleted:
			key := resultKeyOf(st, msg)
			if resultExists(s3Client, bucketName, key) {
				log.Printf("[DEDUPE] Воркер завершил задачу %s (результат: %s) — подтверждаю без пересчёта", msg.TaskID, key)
				return true
			}
			log.Printf("[DEDUPE] Статус completed без результата %s — беру задачу на себя", key)
			return false
		case st.Status != statusRunning:
			log.Printf("[DEDUPE] Воркер завершился со статусом %q — беру задачу на себя", st.Status)
			return false
		}
		upd, err := time.Parse(time.RFC3339, st.UpdatedAt)
		if err != nil || time.Since(upd) > heartbeatStaleAfter {
			// Воркер умер — проверяем, не остался ли готовый результат
			if tryRepairFromResult(s3Client, bucketName, msg) {
				return true
			}
			log.Printf("[DEDUPE] Пульс воркера устарел (updated_at=%s) — перехватываю задачу", st.UpdatedAt)
			return false
		}
	}
	log.Printf("[DEDUPE] Ожидание превысило %s — беру задачу на себя", dedupeMaxWait)
	return false
}

// requeueDelay — пауза перед возвратом сообщения в очередь.
// Потребитель один, поэтому RabbitMQ отдаст сообщение обратно немедленно;
// без паузы повтор попал бы в то же окно недоступности сети.
const requeueDelay = 60 * time.Second

// processMessage выполняет полный цикл обработки одного сообщения:
// скачивание входных данных, расчёт AP3DMT, конвертация и загрузка результата в S3.
// На каждом переходе публикует текущий этап через reporter.
// Тип возвращаемой ошибки определяет судьбу сообщения в очереди: временная —
// сообщение возвращается в очередь, permanent — отбрасывается.
func processMessage(s3Client *s3.Client, bucketName string, message QueueMessage, reporter *statusReporter) error {
	if message.S3FileKey == "" {
		return permanent("message does not contain 's3_file_key'")
	}

	localFileName := filepath.Base(message.S3FileKey)
	if localFileName == "." || localFileName == "/" {
		return permanent("invalid S3 key format, could not extract filename: %s", message.S3FileKey)
	}

	// 5. Определяем путь для локального сохранения
	downloadDir := os.Getenv("APP_DIR")
	if downloadDir == "" {
		downloadDir = "/app/AP3DMT_app/application"
	}
	localFilePath := filepath.Join(downloadDir, localFileName)

	// 6. Скачиваем файл из S3
	reporter.setStage(stageDownloading, "Загрузка входных данных из хранилища")
	if err := retryTransient("S3 download", func() error {
		return downloadFileFromS3(s3Client, bucketName, message.S3FileKey, localFilePath)
	}); err != nil {
		return fmt.Errorf("failed to download file from S3 (key: %s): %w", message.S3FileKey, err)
	}

	log.Printf("Successfully downloaded file: %s", localFilePath)

	// Проверка размера скачанного файла
	if fileInfo, err := os.Stat(localFilePath); err == nil {
		log.Printf("[DIAGNOSTICS] Downloaded file size: %d bytes", fileInfo.Size())
	}

	ext := strings.ToLower(filepath.Ext(localFilePath))
	if ext == ".zip" {
		reporter.setStage(stageExtracting, "Распаковка архива с входными данными")
		log.Printf("Detected ZIP archive: %s. Extracting to: %s", localFilePath, downloadDir)

		// Очистка старых файлов перед распаковкой
		log.Printf("[CLEANUP] Removing old files from previous runs")
		filesToClean := []string{
			"all_models.zip",
			"Nlcg Log File.dat",
			"Output LogFile.dat",
			"result.vtr",
			"model.vtm",
		}
		for _, fname := range filesToClean {
			oldPath := filepath.Join(downloadDir, fname)
			if err := os.Remove(oldPath); err == nil {
				log.Printf("[CLEANUP] Removed old file: %s", fname)
			}
		}
		// Очистка директории Out_Output
		outDir := filepath.Join(downloadDir, "Out_Output")
		if err := os.RemoveAll(outDir); err == nil {
			log.Printf("[CLEANUP] Removed old output directory: Out_Output")
		}

		// Распаковываем файлы прямо в downloadDir (AP3DMT_app/application)
		if err := unzipToFlatDir(localFilePath, downloadDir); err != nil {
			// Битый архив повторной обработкой не исправить
			return permanent("failed to unzip file %s: %v", localFilePath, err)
		}
		log.Printf("Successfully extracted archive contents to: %s", downloadDir)

		// Список файлов после распаковки
		extractedFiles, _ := os.ReadDir(downloadDir)
		log.Printf("[DIAGNOSTICS] Files after extraction (%d total):", len(extractedFiles))
		for i, f := range extractedFiles {
			if i < 15 {
				fInfo, _ := f.Info()
				size := int64(0)
				if fInfo != nil {
					size = fInfo.Size()
				}
				log.Printf("  - %s (size: %d bytes)", f.Name(), size)
			}
		}
		if len(extractedFiles) > 15 {
			log.Printf("  ... and %d more files", len(extractedFiles)-15)
		}

		// Опционально: удалить архив после распаковки, чтобы не занимать место
		os.Remove(localFilePath)
	} else {
		log.Printf("[INFO] File is not a ZIP archive, extension: %s", ext)
	}

	// 7. Запустить внешнюю программу AP3DMT
	reporter.setStage(stageComputing, "Выполняется расчёт AP3DMT")
	log.Printf("[STAGE] Starting AP3DMT execution for user: %s, solution: %s", message.UserID, message.SolutionID)
	if err := call_solve(); err != nil {
		// Расчёт детерминирован по входным данным: повтор сожжёт те же часы CPU с тем же результатом
		return permanent("AP3DMT execution failed for file %s: %v", localFilePath, err)
	}
	log.Printf("[STAGE] AP3DMT execution completed successfully")

	// 8. Запустить Python-скрипт (через venv)
	reporter.setStage(stageConverting, "Конвертация результатов расчёта")
	if err := runPythonScript(); err != nil {
		return permanent("python script execution failed: %v", err)
	}

	// 9. Проверить и загрузить результат (all_models.zip) в S3
	//message.UserID, "_", message.SolutionID,
	resultFileName := "all_models.zip"
	resultLocalPath := filepath.Join(downloadDir, resultFileName)

	// Проверяем, что файл действительно создался
	if _, err := os.Stat(resultLocalPath); os.IsNotExist(err) {
		// 🔍 Отладка: выводим список файлов в директории, чтобы понять, что создалось
		files, _ := os.ReadDir(downloadDir)
		for _, f := range files {
			log.Printf("  Файл в папке: %s", f.Name())
		}
		return permanent("ожидаемый файл %s не найден после выполнения скрипта", resultLocalPath)
	}

	// Формируем ключ для S3
	resultS3Key := fmt.Sprintf("users/%s/solutions/%s/results/%s", message.UserID, message.SolutionID, resultFileName)

	// Загружаем zip-архив в S3
	reporter.setStage(stageUploading, "Загрузка результата в хранилище")
	if err := retryTransient("S3 upload", func() error {
		return uploadFileToS3(s3Client, bucketName, resultLocalPath, resultS3Key)
	}); err != nil {
		return fmt.Errorf("failed to upload result file to S3 (key: %s): %w", resultS3Key, err)
	}

	log.Printf("Successfully uploaded %s to S3: %s", resultFileName, resultS3Key)

	reporter.complete(resultS3Key)

	// 10. Отправить ссылку в Firebase (заглушка)
	log.Printf("Would send link for user: %s, solution: %s, vtr_key: %s", message.UserID, message.SolutionID, resultS3Key)
	log.Printf("Successfully processed message and uploaded result: %s", resultS3Key)
	return nil
}

// handleDelivery обрабатывает одно сообщение очереди: разбор, защита от
// дублей, расчёт и подтверждение. Логика ack/nack определяет судьбу сообщения:
// успех и permanent-ошибки — отбрасываем, временная ошибка первой доставки —
// возвращаем в очередь, временная ошибка повторной доставки — отбрасываем.
func handleDelivery(s3Client *s3.Client, bucketName string, d amqp.Delivery) {
	log.Printf("Received a message: %s", d.Body)

	// Разбираем сообщение здесь, а не внутри processMessage: без user_id
	// и solution_id некуда публиковать статус
	var message QueueMessage
	if err := json.Unmarshal(d.Body, &message); err != nil {
		log.Printf("[QUEUE] Failed to unmarshal message JSON: %v. Body: %s", err, d.Body)
		if rejErr := d.Reject(false); rejErr != nil {
			log.Printf("[QUEUE] Failed to reject message: %v", rejErr)
		}
		return
	}
	if message.UserID == "" || message.SolutionID == "" {
		log.Printf("[QUEUE] Message lacks user_id or solution_id, cannot report status. Body: %s", d.Body)
		if rejErr := d.Reject(false); rejErr != nil {
			log.Printf("[QUEUE] Failed to reject message: %v", rejErr)
		}
		return
	}

	// Защита от дублей: сообщение могло вернуться в очередь после обрыва
	// соединения/ack-сбоя, пока расчёт уже шёл или даже завершился.
	// Повторный многочасовой расчёт той же задачи — огромные потери CPU.
	if checkDuplicate(s3Client, bucketName, message) {
		if ackErr := d.Ack(false); ackErr != nil {
			log.Printf("[QUEUE] Failed to ack duplicate: %v", ackErr)
		}
		return
	}

	reporter := newStatusReporter(s3Client, bucketName, message)
	// Пульс держим на всё время обработки: самый долгий этап (расчёт)
	// не публикует ничего сам, и без обновления updated_at основной
	// сервер не поймёт, жив ли потребитель
	reporter.startHeartbeat()

	err := processMessage(s3Client, bucketName, message, reporter)

	switch {
	case err == nil:
		// Статус completed уже опубликован внутри processMessage
		if ackErr := d.Ack(false); ackErr != nil {
			// Ack не прошёл (канал закрыт брокером) — сообщение вернётся в очередь,
			// но следующая доставка будет пропущена через checkDuplicate
			log.Printf("[QUEUE] Failed to ack message: %v — возможна повторная доставка (будет пропущена [DEDUPE])", ackErr)
		}

	case isPermanent(err):
		// Повтор даст тот же результат — отбрасываем, чтобы не блокировать очередь
		log.Printf("[QUEUE] Permanent failure, message discarded: %v", err)
		log.Printf("[QUEUE] Discarded message body: %s", d.Body)
		reporter.fail(err, false)
		if rejErr := d.Reject(false); rejErr != nil {
			log.Printf("[QUEUE] Failed to reject message: %v", rejErr)
		}

	case d.Redelivered:
		// Сообщение уже возвращалось в очередь и упало снова — не гоняем его по кругу
		log.Printf("[QUEUE] Transient failure on redelivered message, giving up: %v", err)
		log.Printf("[QUEUE] Message body (requires manual re-publish): %s", d.Body)
		reporter.fail(err, false)
		if rejErr := d.Reject(false); rejErr != nil {
			log.Printf("[QUEUE] Failed to reject message: %v", rejErr)
		}

	default:
		// Временный сбой на первой доставке — возвращаем сообщение в очередь
		log.Printf("[QUEUE] Transient failure: %v", err)
		log.Printf("[QUEUE] Requeueing message in %s", requeueDelay)
		reporter.fail(err, true)
		time.Sleep(requeueDelay)
		if nackErr := d.Nack(false, true); nackErr != nil {
			log.Printf("[QUEUE] Failed to nack/requeue message: %v", nackErr)
		}
	}

	reporter.stop()
}

// consumeLoop подключается к RabbitMQ и обрабатывает сообщения, пока
// соединение или канал не закроются (обрыв сети, consumer_timeout брокера,
// перезапуск RabbitMQ). Любая ошибка возвращается супервизору в main(),
// который переподключается — потребитель не должен умирать молча.
func consumeLoop(s3Client *s3.Client, bucketName, rabbitURL string) error {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"calculation_tasks", // имя очереди
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	// Забираем ровно одно сообщение за раз: расчёт длится долго, а при ручном
	// подтверждении без этого ограничения брокер сразу отдал бы всю очередь
	// одному потребителю, и при его падении неподтверждённые сообщения зависли бы.
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("failed to set channel QoS: %w", err)
	}

	// Уникальный consumer tag: после переподключения старый тег может ещё
	// числиться у брокера, пока он не заметил обрыв
	consumerTag := fmt.Sprintf("mtz-consumer-%d", time.Now().UnixNano())

	msgs, err := ch.Consume(
		q.Name,      // queue (используем имя из q)
		consumerTag, // consumer
		false,       // auto-ack выключен: подтверждаем только после успешной обработки
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	// Событие закрытия соединения — чтобы не ждать молчаливого закрытия msgs
	// (оно тоже закроется, но NotifyClose даёт ясную причину для лога)
	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))

	log.Printf("[QUEUE] Connected to RabbitMQ, waiting for messages (consumer tag: %s). To exit press CTRL+C", consumerTag)

	for {
		select {
		case amqpErr := <-connClosed:
			if amqpErr == nil {
				return errors.New("connection closed gracefully")
			}
			return fmt.Errorf("connection lost: [%d] %s", amqpErr.Code, amqpErr.Reason)
		case d, ok := <-msgs:
			if !ok {
				// Брокер закрыл канал потребителя (например, consumer_timeout
				// для неподтверждённого сообщения) — переподключаемся
				return errors.New("consumer channel closed by broker (possible consumer_timeout on long unacked delivery)")
			}
			handleDelivery(s3Client, bucketName, d)
		}
	}
}

func main() {
	s3Client, err := initS3Client()
	if err != nil {
		log.Fatalf("Failed to initialize S3 client: %v", err)
	}
	log.Println("S3 client initialized successfully.")

	// Имя бакета уже проверено в initS3Client, читаем его один раз:
	// он нужен и для данных решения, и для публикации статуса
	bucketName := os.Getenv("AWS_S3_BUCKET_NAME")

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@10.42.0.65:5672/" // fallback
	}

	// Бесконечный цикл переподключения: расчёт идёт часами, за это время
	// соединение с RabbitMQ может оборваться (сеть, consumer_timeout,
	// перезапуск брокера). Без переподключения процесс остался бы жив,
	// но задачи больше не брал — и на дашборде пропали бы активные серверы.
	const maxReconnectPause = 60 * time.Second
	reconnectPause := 5 * time.Second

	for {
		started := time.Now()
		err := consumeLoop(s3Client, bucketName, rabbitURL)
		log.Printf("[QUEUE] Consumer stopped: %v", err)

		if time.Since(started) > time.Minute {
			reconnectPause = 5 * time.Second // соединение было устойчивым — сбрасываем backoff
		}
		log.Printf("[QUEUE] Reconnecting in %s", reconnectPause)
		time.Sleep(reconnectPause)
		if reconnectPause < maxReconnectPause {
			reconnectPause *= 2
		}
	}
}
