FROM golang:1.24-bookworm AS build

WORKDIR /etracker

COPY go.* .
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-w -s" -o etracker ./cmd/etracker

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etracker/etracker /usr/bin/

EXPOSE 8080

ENTRYPOINT ["/usr/bin/etracker"]
CMD ["start", "--port", "8080"]
