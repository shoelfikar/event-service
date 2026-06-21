# ── build ────────────────────────────────────────────────────────────
FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/event-service ./cmd/event-service

# ── runtime ──────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/event-service /app/event-service
# Mount or COPY the GeoLite2-City.mmdb and point GEOIP_CITY_DB_PATH at it.
USER nonroot:nonroot
EXPOSE 4100
ENTRYPOINT ["/app/event-service"]
