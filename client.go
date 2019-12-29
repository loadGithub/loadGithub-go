package main

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	_"loadgithub/service"
	"log"
	"os"
	"github.com/apache/pulsar-client-go/pulsar"
	"fmt"
)

func main() {

	grpcServer := os.Getenv("GRPC_SERVER")
	log.Println("grpcServer:" + grpcServer)
	log.Println(grpcServer == "")

	pulsarBroker := os.Getenv("PULSAR_BROKER")

	grpcServer = "192.168.1.8:5560"

	if grpcServer==""{
		grpcServer = "grpcServer:5560"
	}

	pulsarBroker = "192.168.1.6:6650"

	if pulsarBroker==""{
		pulsarBroker = "pulsar:6650"
	}

	role := os.Getenv("LHG_ROLE")
	if role == "" {
		role = "worker"
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://"+pulsarBroker,
	})

	if err!=nil{
		log.Println("create pulsar client fail:{}",err)
	}

	defer client.Close()

	if role == "worker" {

		go func(){
			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				// topic lgh/viewer/{tokener}
				Topic:            "persistent://lgh/viewer/default",
				// message base
				//1. token  ->  lgh_token_default  --> etcd
				//2. login
				//3.followingEndCursor
				//4.followerEndCursor
	
				//extra 
				//1.following limit
				//2.follower limit
				SubscriptionName: "my-sub3",
				Type:             pulsar.Shared,
			})
	
			defer consumer.Close()

			// 建立连接到gRPC服务
			conn, err := grpc.Dial(grpcServer, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			// 函数结束时关闭连接
			defer conn.Close()

			for{

				msg, err := consumer.Receive(context.Background())
				if err != nil {
					log.Fatal(err)
				}
			
				fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
				msg.ID(), string(msg.Payload()))

				// var queryFollowRequest QueryFollowRequest
		
				// rdr := strings.NewReader(string(msg.Payload()))
				//  将json串rdr解码到结构体对象p1中
				// json.NewDecoder(rdr).Decode(&queryFollowRequest)

				// 创建Waiter服务的客户端
				// t := service.NewGreeterClient(conn)

				// grpcClient := service.NewGithubLoaderClient(conn)

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

				// resp, err := grpcClient.QueryFollow(context.Background(), &service.QueryFollowRequest{Login: "", Token: "", FollowingEndCursor: "", FollowerEndCursor: ""})
				// if err != nil {
				// 	log.Fatalf("could not queryFollow: %v", err)
				// }
				// log.Printf("queryFollow.result: %s", resp.Data)

			}
		}()

	}

	select{

	}

	
}
