FROM golang:latest

#国内代理加速 
ENV GOPROXY=https://goproxy.cn

WORKDIR $GOPATH/src/loadgithub.net/shibalnu
COPY . $GOPATH/src/loadgithub.net/shibalnu
RUN go build -o shibalnu client.go 
#删除不必要的内容
RUN rm -f *.go && rm -f Dockerfile && rm -f README.md
 
#EXPOSE 10086
ENTRYPOINT ["./shibalnu"]