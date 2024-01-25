FROM spbgit.polymetal.ru:5005/polyna/docker/images/asd-golang:1.2

WORKDIR usr/local/src/service/messdeliv

COPY ["go.mod", "go.sum", "./"]

RUN go mod download

COPY . .

RUN go build -o /go/src/service/messdeliv main.go

USER asd:asd
