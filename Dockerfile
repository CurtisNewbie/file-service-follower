FROM golang:1.18
LABEL author="yongjie.zhuang"
LABEL descrption="File Service Follower - for backing up file-server's data"

ENV TZ=Asia/Shanghai

WORKDIR /usr/src/file-service-follower

# for golang env
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://mirrors.aliyun.com/goproxy/,direct

# dependencies
COPY go.mod .
COPY go.sum .

RUN go mod download

# build executable
COPY . .

RUN go build -o ./main

# script (for io redirection stuff)
# COPY run.sh ./ 
RUN chmod +x run.sh

CMD ["sh", "run.sh"]
