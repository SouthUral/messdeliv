FROM spbgit.polymetal.ru:5005/polyna/docker/images/asd-golang:1.2

WORKDIR /usr/local/go/src/messdeliv

# RUN export GOROOT=/usr/local/go/src/messdeliv

COPY ["go.mod", "go.sum", "./"]

RUN go mod download

COPY . .

RUN ls

RUN go build -o /usr/local/go/src/messdeliv main.go

USER asd:asd
