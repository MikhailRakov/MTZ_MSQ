// main consumer.go
package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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

	// Загружаем конфигурацию
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credsProvider),
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

	result, err := s3Client.GetObject(context.TODO(), input)
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

	putObjectInput := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Body:   file,
		// ACL:  types.ObjectCannedACL("private"), // Установите ACL по необходимости
	}

	_, err = s3Client.PutObject(context.TODO(), putObjectInput)
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

	cmd := exec.Command("./run_AP3DMT_comp.sh", mcrPath)
	cmd.Dir = appDir

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
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

	msgs, err := ch.Consume(
		q.Name,    // queue (используем имя из q)
		"server1", // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

			var message QueueMessage
			if err := json.Unmarshal(d.Body, &message); err != nil {
				log.Printf("Failed to unmarshal message JSON: %v. Body: %s", err, string(d.Body))
				continue
			}

			if message.S3FileKey == "" {
				log.Printf("Message does not contain 's3_file_key'. Skipping. Message: %s", string(d.Body))
				continue
			}

			localFileName := filepath.Base(message.S3FileKey)
			if localFileName == "." || localFileName == "/" {
				log.Printf("Invalid S3 key format, could not extract filename: %s. Skipping.", message.S3FileKey)
				continue
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
				log.Printf("AWS_S3_BUCKET_NAME environment variable is not set. Cannot download file with key: %s", message.S3FileKey)
				continue
			}

			if err := downloadFileFromS3(s3Client, bucketName, message.S3FileKey, localFilePath); err != nil {
				log.Printf("Failed to download file from S3 (key: %s): %v", message.S3FileKey, err)
				continue
			}

			log.Printf("Successfully downloaded file: %s", localFilePath)

			ext := strings.ToLower(filepath.Ext(localFilePath))
			if ext == ".zip" {
				log.Printf("Detected ZIP archive: %s. Extracting to: %s", localFilePath, downloadDir)

				// Распаковываем файлы прямо в downloadDir (AP3DMT_app/application)
				if err := unzipToFlatDir(localFilePath, downloadDir); err != nil {
					log.Printf("Failed to unzip file %s: %v", localFilePath, err)
					continue
				}
				log.Printf("Successfully extracted archive contents to: %s", downloadDir)

				// Опционально: удалить архив после распаковки, чтобы не занимать место
				os.Remove(localFilePath)
			}

			// 7. Запустить внешнюю программу AP3DMT
			if err := call_solve(); err != nil {
				log.Printf("AP3DMT execution failed for file %s. Skipping further processing. Error: %v", localFilePath, err)
				continue // Пропускаем дальнейшую обработку, если AP3DMT завершился с ошибкой
			}

			// 8. Запустить Python-скрипт (через venv)
			if err := runPythonScript(); err != nil {
				log.Printf("Python script execution failed. Error: %v", err)
				continue
			}

			// 9. Проверить и загрузить результат (all_models.zip) в S3
			//message.UserID, "_", message.SolutionID,
			resultFileName := "all_models.zip"
			resultLocalPath := filepath.Join(downloadDir, resultFileName)

			// Проверяем, что файл действительно создался
			if _, err := os.Stat(resultLocalPath); os.IsNotExist(err) {
				log.Printf("Ожидаемый файл %s не найден после выполнения скрипта!", resultLocalPath)

				// 🔍 Отладка: выводим список файлов в директории, чтобы понять, что создалось
				files, _ := os.ReadDir(downloadDir)
				for _, f := range files {
					log.Printf("  Файл в папке: %s", f.Name())
				}
				continue
			}

			// Формируем ключ для S3
			resultS3Key := fmt.Sprintf("users/%s/solutions/%s/results/%s", message.UserID, message.SolutionID, resultFileName)

			// Загружаем zip-архив в S3
			if err := uploadFileToS3(s3Client, bucketName, resultLocalPath, resultS3Key); err != nil {
				log.Printf("Failed to upload result file to S3 (key: %s): %v", resultS3Key, err)
				continue
			}

			log.Printf("Successfully uploaded %s to S3: %s", resultFileName, resultS3Key)

			// 10. Отправить ссылку в Firebase (заглушка)
			log.Printf("Would send link for user: %s, solution: %s, vtr_key: %s", message.UserID, message.SolutionID, resultS3Key)
			log.Printf("Successfully processed message and uploaded result: %s", resultS3Key)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
