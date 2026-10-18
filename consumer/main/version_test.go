package main

import (
	"testing"
	"time"
)

// TestRunVersion проверяет детерминированность версии запуска: одинаковый
// timestamp сообщения (исходная доставка и редоставления) даёт одинаковый
// каталог результата — повторная обработка не плодит копии архива.
func TestRunVersion(t *testing.T) {
	ts := time.Date(2026, 2, 1, 16, 20, 52, 0, time.UTC).Unix()
	msg := QueueMessage{UserID: "u1", SolutionID: "s1", Timestamp: ts}

	if v := runVersion(msg); v != "run-20260201-162052" {
		t.Fatalf("runVersion = %s, want run-20260201-162052", v)
	}

	// Детерминизм между вызовами
	if runVersion(msg) != runVersion(msg) {
		t.Fatal("runVersion не детерминирована")
	}

	// Без timestamp — запасной вариант (текущее время), формат сохраняется
	fallback := runVersion(QueueMessage{})
	if len(fallback) != len("run-20060102-150405") {
		t.Fatalf("fallback version = %s, unexpected format", fallback)
	}

	want := "users/u1/solutions/s1/results/run-20260201-162052/all_models.zip"
	if k := defaultResultKey(msg); k != want {
		t.Fatalf("defaultResultKey = %s, want %s", k, want)
	}

	wantLegacy := "users/u1/solutions/s1/results/all_models.zip"
	if k := legacyResultKey(msg); k != wantLegacy {
		t.Fatalf("legacyResultKey = %s, want %s", k, wantLegacy)
	}
}
