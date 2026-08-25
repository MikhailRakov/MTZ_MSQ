// main consumer.go
package main

import (
	"archive/zip"
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

	// Получаем размер файла для логирования
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

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
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

// requeueDelay — пауза перед возвратом сообщения в очередь.
// Потребитель один, поэтому RabbitMQ отдаст сообщение обратно немедленно;
// без паузы повтор попал бы в то же окно недоступности сети.
const requeueDelay = 60 * time.Second

// processMessage выполняет полный цикл обработки одного сообщения:
// скачивание входных данных, расчёт AP3DMT, конвертация и загрузка результата в S3.
// Тип возвращаемой ошибки определяет судьбу сообщения в очереди: временная —
// сообщение возвращается в очередь, permanent — отбрасывается.
func processMessage(s3Client *s3.Client, body []byte) error {
	var message QueueMessage
	if err := json.Unmarshal(body, &message); err != nil {
		return permanent("failed to unmarshal message JSON: %v", err)
	}

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
	bucketName := os.Getenv("AWS_S3_BUCKET_NAME")
	if bucketName == "" {
		// Ошибка конфигурации, а не данных: сообщение должно дожить до исправления переменных
		return fmt.Errorf("AWS_S3_BUCKET_NAME environment variable is not set, cannot download key: %s", message.S3FileKey)
	}

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
	log.Printf("[STAGE] Starting AP3DMT execution for user: %s, solution: %s", message.UserID, message.SolutionID)
	if err := call_solve(); err != nil {
		// Расчёт детерминирован по входным данным: повтор сожжёт те же часы CPU с тем же результатом
		return permanent("AP3DMT execution failed for file %s: %v", localFilePath, err)
	}
	log.Printf("[STAGE] AP3DMT execution completed successfully")

	// 8. Запустить Python-скрипт (через venv)
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
	if err := retryTransient("S3 upload", func() error {
		return uploadFileToS3(s3Client, bucketName, resultLocalPath, resultS3Key)
	}); err != nil {
		return fmt.Errorf("failed to upload result file to S3 (key: %s): %w", resultS3Key, err)
	}

	log.Printf("Successfully uploaded %s to S3: %s", resultFileName, resultS3Key)

	// 10. Отправить ссылку в Firebase (заглушка)
	log.Printf("Would send link for user: %s, solution: %s, vtr_key: %s", message.UserID, message.SolutionID, resultS3Key)
	log.Printf("Successfully processed message and uploaded result: %s", resultS3Key)
	return nil
}

func main() {
	s3Client, err := initS3Client()
	if err != nil {
		log.Fatalf("Failed to initialize S3 client: %v", err)
	}
	log.Println("S3 client initialized successfully.")

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@10.42.0.65:5672/" // fallback
	}

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"calculation_tasks", // имя очереди
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Забираем ровно одно сообщение за раз: расчёт длится долго, а при ручном
	// подтверждении без этого ограничения брокер сразу отдал бы всю очередь
	// одному потребителю, и при его падении неподтверждённые сообщения зависли бы.
	err = ch.Qos(1, 0, false)
	failOnError(err, "Failed to set channel QoS")

	msgs, err := ch.Consume(
		q.Name,    // queue (используем имя из q)
		"server1", // consumer
		false,     // auto-ack выключен: подтверждаем только после успешной обработки
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever = make(chan struct{})

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

			err := processMessage(s3Client, d.Body)

			switch {
			case err == nil:
				if ackErr := d.Ack(false); ackErr != nil {
					log.Printf("[QUEUE] Failed to ack message: %v", ackErr)
				}

			case isPermanent(err):
				// Повтор даст тот же результат — отбрасываем, чтобы не блокировать очередь
				log.Printf("[QUEUE] Permanent failure, message discarded: %v", err)
				log.Printf("[QUEUE] Discarded message body: %s", d.Body)
				if rejErr := d.Reject(false); rejErr != nil {
					log.Printf("[QUEUE] Failed to reject message: %v", rejErr)
				}

			case d.Redelivered:
				// Сообщение уже возвращалось в очередь и упало снова — не гоняем его по кругу
				log.Printf("[QUEUE] Transient failure on redelivered message, giving up: %v", err)
				log.Printf("[QUEUE] Message body (requires manual re-publish): %s", d.Body)
				if rejErr := d.Reject(false); rejErr != nil {
					log.Printf("[QUEUE] Failed to reject message: %v", rejErr)
				}

			default:
				// Временный сбой на первой доставке — возвращаем сообщение в очередь
				log.Printf("[QUEUE] Transient failure: %v", err)
				log.Printf("[QUEUE] Requeueing message in %s", requeueDelay)
				time.Sleep(requeueDelay)
				if nackErr := d.Nack(false, true); nackErr != nil {
					log.Printf("[QUEUE] Failed to nack/requeue message: %v", nackErr)
				}
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	<-forever
}
