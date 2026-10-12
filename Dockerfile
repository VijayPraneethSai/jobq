FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/coordinator ./cmd/worker ./cmd/jobqctl
# distroless has no shell, so create the data dir here and copy it across with the right owner (65532 = nonroot)
RUN mkdir /data-empty

# Worker image: has a shell and coreutils so jobs like `sh -c ...` can actually run.
FROM alpine:3.20 AS worker
RUN adduser -D -u 65532 jobq
COPY --from=build /out/ /usr/local/bin/
USER jobq
ENTRYPOINT ["/usr/local/bin/worker"]
CMD ["-coordinator", "http://coordinator:8080"]

# Coordinator image (last stage, so a plain `docker build .` produces this one).
FROM gcr.io/distroless/static-debian12:nonroot AS coordinator
COPY --from=build /out/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /data-empty /data
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/coordinator"]
CMD ["-addr", ":8080", "-data", "/data"]
