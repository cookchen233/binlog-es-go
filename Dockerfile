# ---------------------------
# Build Stage
# ---------------------------
FROM golang:1.24.4-alpine AS builder
WORKDIR /app

RUN apk add --no-cache git

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o binlog-es-go ./cmd/binlog-es-go


# ---------------------------
# Runtime Stage
# ---------------------------
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata wget \
 && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /app

# 运行时二进制
COPY --from=builder /app/binlog-es-go .

# 默认数据目录：用于保存位点、日志和其他运行产物
RUN mkdir -p /data/binlog-es-go \
    /data/binlog-es-go/logs \
    /data/binlog-es-go/position

# 默认环境变量（可在 docker-compose 中覆盖）
ENV BEG_CONFIG=/app/configs/config.yaml \
    BEG_MODE=realtime \
    BEG_TASK=sheet1 \
    BEG_SERVER_PORT=8222 \
    TZ=Asia/Shanghai

# 默认暴露 HTTP 端口（healthz/metrics）
EXPOSE 8222

# 健康检查：依赖内置 /healthz
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8222/healthz || exit 1

# 允许通过环境变量覆盖 config/mode/task 等
ENTRYPOINT ["/app/binlog-es-go"]
CMD ["--config=/app/configs/config.yaml", "--mode=realtime", "--task=sheet1"]
