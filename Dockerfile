# ---- 构建阶段 ----
FROM golang:1.26-alpine AS builder

WORKDIR /src

# 先拷贝依赖清单并下载，利用镜像层缓存
COPY go.mod go.sum ./
RUN go mod download

# 拷贝源码并编译成静态二进制：
# - web/ 目录被 go:embed 打进二进制
# - timetzdata 标签把时区库嵌入二进制，运行镜像无需 tzdata 包
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -tags timetzdata -trimpath -ldflags="-s -w" -o /out/monitor .

# ---- 运行阶段 ----
FROM alpine:3.20

# 只需 CA 证书用于 https 请求上游，直接从构建镜像拷贝，避免运行阶段联网装包
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

WORKDIR /app
COPY --from=builder /out/monitor /app/monitor

# 容器内固定监听 8080，对外发布端口由 docker-compose 映射
EXPOSE 8080

# config.yaml 与 data/ 目录通过挂载卷提供
ENTRYPOINT ["/app/monitor", "/app/config.yaml"]
