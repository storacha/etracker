FROM golang:1.24-bookworm AS build

WORKDIR /etracker

COPY go.* .
RUN go mod download
COPY . .

ARG VERSION
ARG DATE

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
	go build -ldflags="-w -s -X github.com/storacha/etracker/internal/build.version=$VERSION -X github.com/storacha/etracker/internal/build.Date=$DATE -X github.com/storacha/etracker/internal/build.BuiltBy=docker" \
    -o etracker github.com/storacha/etracker/cmd/etracker

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etracker/etracker /usr/bin/

EXPOSE 8080

ENTRYPOINT ["/usr/bin/etracker"]
CMD ["start", "--port", "8080"]
