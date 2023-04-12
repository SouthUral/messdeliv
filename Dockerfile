FROM golang:latest

ENV USER=asd
ARG USER_ID=1000
ARG GROUP_ID=1000
# ARG GIT_URL=spbgit.polymetal.ru
# ENV GOPROXY=http://${GIT_URL}:4872

# RUN addgroup -g ${GROUP_ID} ${USER} &&\
#      adduser -D -u ${USER_ID} -G ${USER} ${USER}
#
WORKDIR /go/src/messdeliv/
#
COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./
## Build the binary.
RUN CGO_ENABLED=0 GOOS=linux go build -o /mess_deliv

CMD ["/mess_deliv"]

# RUN go build -o /go/src/messDeliv main.go

# FROM alpine:3.17

# RUN apk add --no-cache tzdata

# WORKDIR /go/src/eventNotifyer/
#
## Import the user and group files from the builder.
# COPY --from=builder /etc/passwd /etc/passwd
# COPY --from=builder /etc/group /etc/group

## Run the hello binary.
# COPY --chown=asd:asd --from=builder /go/src/eventNotifyer/eNotifyer /go/src/eventNotifyer/eNotifyer

# USER asd:asd

# CMD ["/go/src/messDeliv/main"]