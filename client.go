package main

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"loadgithub/service"
	"log"
	"os"
	// "github.com/apache/pulsar-client-go/pulsar"
)

func main() {

	grpcHost := os.Getenv("GRPC_HOST")
	log.Println("grpcHost:" + grpcHost)
	log.Println(grpcHost == "")

	role := os.Getenv("LHG_ROLE")
	if role == "" {
		role = "worker"
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	defer client.Close()

	if role == "worker" {

		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            "lgh_viewer_default",
			SubscriptionName: "my-sub",
			Type:             pulsar.Shared,
		})

		defer consumer.Close()

		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))
	}

	// log.Println(grpcHost==nil)

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
	resp, err := client.QueryFollow(context.Background(), &service.QueryFollowRequest{Login: "", Token: "", FollowingEndCursor: "", FollowerEndCursor: ""})
	if err != nil {
		log.Fatalf("could not queryFollow: %v", err)
	}
	log.Printf("queryFollow.result: %s", resp.Data)
}
