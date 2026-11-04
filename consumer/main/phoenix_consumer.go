// phoenix_consumer.go — обработчик очереди phoenix_conversion_tasks.
// Скачивает zip-архив сырых данных Phoenix из S3, распаковывает в рабочую директорию
// решения и публикует задачу в очередь calculation_tasks для расчёта AP3DMT.
//
// Поток: raw-upload (MTZ_S) → phoenix_conversion_tasks → [этот consumer] → calculation_tasks
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	amqp "github.com/rabbitmq/amqp091-go"
)

// PhoenixMessage — структура сообщения из очереди phoenix_conversion_tasks.
// Поля совпадают с тем, что публикует MTZ_S handler UploadRawPhoenixData.
type PhoenixMessage struct {
	UserID      string `json:"user_id"`
	SolutionID  string `json:"solution_id"`
	TaskID      string `json:"task_id,omitempty"`
	S3FileKey   string `json:"s3_file_key"`
	MetadataKey string `json:"metadata_key,omitempty"`
	Timestamp   int64  `json:"timestamp"`
}

// processPhoenixMessage обрабатывает одно сообщение из phoenix_conversion_tasks:
// 1. Скачивает zip из S3
// 2. Распаковывает в рабочую директорию решения
// 3. Публикует задачу в calculation_tasks
func processPhoenixMessage(s3Client *s3.Client, bucketName string, msg PhoenixMessage, ch *amqp.Channel) error {
	if msg.S3FileKey == "" {
		return permanent("phoenix message missing s3_file_key")
	}
	if msg.UserID == "" || msg.SolutionID == "" {
		return permanent("phoenix message missing user_id or solution_id")
	}

	log.Printf("[PHOENIX] Processing raw data for user=%s solution=%s task=%s", msg.UserID, msg.SolutionID, msg.TaskID)

	// 1. Скачать zip из S3
	localFileName := filepath.Base(msg.S3FileKey)
	downloadDir := os.Getenv("APP_DIR")
	if downloadDir == "" {
		downloadDir = "/app/AP3DMT_app/application"
	}
	localFilePath := filepath.Join(downloadDir, localFileName)

	log.Printf("[PHOENIX] Downloading %s from S3", msg.S3FileKey)
	if err := retryTransient("S3 download (phoenix)", func() error {
		return downloadFileFromS3(s3Client, bucketName, msg.S3FileKey, localFilePath)
	}); err != nil {
		return fmt.Errorf("failed to download phoenix zip: %w", err)
	}

	// Проверка размера
	if info, err := os.Stat(localFilePath); err == nil {
		log.Printf("[PHOENIX] Downloaded zip size: %.2f MB", float64(info.Size())/(1024*1024))
	}

	// 2. Распаковать zip в рабочую директорию решения
	log.Printf("[PHOENIX] Extracting %s to %s", localFilePath, downloadDir)
	if err := unzipToFlatDir(localFilePath, downloadDir); err != nil {
		os.Remove(localFilePath)
		return permanent("failed to extract phoenix zip: %v", err)
	}
	os.Remove(localFilePath) // убираем архив после распаковки

	// Список извлечённых файлов
	extractedFiles, _ := os.ReadDir(downloadDir)
	log.Printf("[PHOENIX] Extracted %d files:", len(extractedFiles))
	for i, f := range extractedFiles {
		if i < 20 {
			log.Printf("  - %s", f.Name())
		}
	}

	// 3. Опубликовать задачу в calculation_tasks
	calcMsg := QueueMessage{
		UserID:     msg.UserID,
		SolutionID: msg.SolutionID,
		TaskID:     msg.TaskID,
		// S3FileKey оставляем пустым: данные уже распакованы локально,
		// processMessage увидит файлы в APP_DIR через unzipToFlatDir.
		// Но для совместимости с processMessage передаём ключ оригинального архива —
		// processMessage попытается скачать и распаковать снова, но это безопасно
		// (файлы перезапишутся теми же).
		S3FileKey: msg.S3FileKey,
		Timestamp: msg.Timestamp,
	}

	body, err := json.Marshal(calcMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal calculation message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",                    // exchange (default)
		"calculation_tasks",   // routing key = queue name
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Timestamp:    time.Now().UTC(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to calculation_tasks: %w", err)
	}

	log.Printf("[PHOENIX] Published task %s to calculation_tasks for user=%s solution=%s",
		msg.TaskID, msg.UserID, msg.SolutionID)
	return nil
}

// consumePhoenixLoop — цикл потребления очереди phoenix_conversion_tasks.
// Аналогичен consumeLoop, но для отдельной очереди и с упрощённой обработкой.
func consumePhoenixLoop(s3Client *s3.Client, bucketName, rabbitURL string) error {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ (phoenix): %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel (phoenix): %w", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"phoenix_conversion_tasks",
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare phoenix queue: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS (phoenix): %w", err)
	}

	consumerTag := fmt.Sprintf("mtz-phoenix-%d", time.Now().UnixNano())
	msgs, err := ch.Consume(q.Name, consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register phoenix consumer: %w", err)
	}

	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	log.Printf("[PHOENIX] Connected, waiting for messages (tag: %s)", consumerTag)

	for {
		select {
		case amqpErr := <-connClosed:
			if amqpErr == nil {
				return fmt.Errorf("phoenix connection closed gracefully")
			}
			return fmt.Errorf("phoenix connection lost: [%d] %s", amqpErr.Code, amqpErr.Reason)
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("phoenix consumer channel closed by broker")
			}
			handlePhoenixDelivery(s3Client, bucketName, d, ch)
		}
	}
}

// handlePhoenixDelivery обрабатывает одно сообщение из phoenix_conversion_tasks.
func handlePhoenixDelivery(s3Client *s3.Client, bucketName string, d amqp.Delivery, ch *amqp.Channel) {
	log.Printf("[PHOENIX] Received message: %s", d.Body)

	var msg PhoenixMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("[PHOENIX] Failed to unmarshal: %v. Body: %s", err, d.Body)
		if rejErr := d.Reject(false); rejErr != nil {
			log.Printf("[PHOENIX] Failed to reject: %v", rejErr)
		}
		return
	}

	err := processPhoenixMessage(s3Client, bucketName, msg, ch)

	switch {
	case err == nil:
		if ackErr := d.Ack(false); ackErr != nil {
			log.Printf("[PHOENIX] Failed to ack: %v", ackErr)
		}
	case isPermanent(err):
		log.Printf("[PHOENIX] Permanent failure, discarding: %v", err)
		if rejErr := d.Reject(false); rejErr != nil {
			log.Printf("[PHOENIX] Failed to reject: %v", rejErr)
		}
	case d.Redelivered:
		log.Printf("[PHOENIX] Transient failure on redelivery, giving up: %v", err)
		if rejErr := d.Reject(false); rejErr != nil {
			log.Printf("[PHOENIX] Failed to reject: %v", rejErr)
		}
	default:
		log.Printf("[PHOENIX] Transient failure, requeueing: %v", err)
		time.Sleep(requeueDelay)
		if nackErr := d.Nack(false, true); nackErr != nil {
			log.Printf("[PHOENIX] Failed to nack: %v", nackErr)
		}
	}
}
