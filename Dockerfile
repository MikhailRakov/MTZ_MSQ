
FROM golang:1.24-bookworm AS builder
WORKDIR /app
COPY consumer/go.mod consumer/go.sum ./
RUN go mod download
COPY consumer/ ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o consumer ./main/consumer.go

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl bash python3 python3-pip \
    libstdc++6 libx11-6 libxext6 libxrender1 libxtst6 \
    libxi6 libsm6 libice6 libgl1 libgomp1 \
    libglib2.0-0 libgtk-3-0 libnss3 libasound2 libxcb1 \
    libdbus-1-3 libxkbcommon0 libatspi2.0-0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libxt6 libxmu6 libxpm4 \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt


RUN useradd -m appuser && \
    mkdir -p /app/data /app/logs && \
    chown -R appuser:appuser /app


COPY --from=builder --chown=appuser:appuser /app/consumer /app/consumer


COPY --chown=appuser:appuser entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV APP_DIR=/app DATA_DIR=/app/data LOG_LEVEL=info
WORKDIR /app
USER appuser

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["./consumer"]
