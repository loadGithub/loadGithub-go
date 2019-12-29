package main

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"loadgithub/service"
	"log"
	"os"
)

func main() {
	// 建立连接到gRPC服务
	conn, err := grpc.Dial("192.168.1.8:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	// 函数结束时关闭连接
	defer conn.Close()

	// 创建Waiter服务的客户端
    // t := service.NewGreeterClient(conn)
    client := service.NewGithubLoaderClient(conn)

	// 模拟请求数据
	// res := "test123"
	// os.Args[1] 为用户执行输入的参数 如：go run ***.go 123
	// if len(os.Args) > 1 {
		// res = os.Args[1]
	// }

	// 调用gRPC接口
	// tr, err := t.SayHello(context.Background(), &service.HelloRequest{Name: res})
	// if err != nil {
	// 	log.Fatalf("could not greet: %v", err)
	// }
    // log.Printf("服务端响应: %s", tr.Message)
    resp,err := client.QueryFollow(context.Background(),&service.QueryFollowRequest{Login="",Token="",FollowingEndCursor="",FollowerEndCursor=""})
    if err!=nil{
        log.Fatalf("could not queryFollow: %v", err)
    }
    log.Printf("queryFollow.result: %s", resp.QueryFollowResp)
}
