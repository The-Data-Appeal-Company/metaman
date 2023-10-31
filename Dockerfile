ARG GO_VERSION=1.20
ARG APP_NAME="metaman"

FROM golang:${GO_VERSION}-alpine AS builder

RUN apk add --no-cache ca-certificates git upx

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY ./ ./

RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w" \
    -installsuffix 'static' \
    -o /app .

RUN upx /app

FROM scratch AS final

COPY --from=builder /app /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app"]