FROM golang:1.21.0-bookworm

ENV GOPATH /go
WORKDIR /go/src/github.com/goccy/kubejob

COPY ./go.* ./

RUN go mod download

COPY . .

RUN go build -o /go/bin/kubejob-agent cmd/kubejob-agent/main.go

FROM golang:1.21.0-bookworm AS agent

COPY --from=0 /go/bin/kubejob-agent /bin/kubejob-agent
