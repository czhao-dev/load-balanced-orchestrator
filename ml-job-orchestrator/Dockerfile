FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /orchestrator ./cmd/orchestrator

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /orchestrator /orchestrator
EXPOSE 8080
ENTRYPOINT ["/orchestrator"]
