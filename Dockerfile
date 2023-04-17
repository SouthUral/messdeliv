FROM spbgit.polymetal.ru:5005/polyna/docker/images/asd-golang:1.2

WORKDIR $GOPATH/src/service/

COPY . .

RUN go build -o /go/src/service/messdeliv main.go

USER asd:asd
