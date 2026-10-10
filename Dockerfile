FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/coordinator ./cmd/worker ./cmd/jobqctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/coordinator"]
CMD ["-addr", ":8080", "-data", "/data"]
