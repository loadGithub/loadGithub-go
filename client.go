package main

import (
	"encoding/json"
	"fmt"
	"loadgithub/service"
	"log"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis/v7"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type LoadgithubMto struct {
	Login              string `json:"login"`
	FollowingEndCursor string `json:"followingEndCursor"`
	FollowerEndCursor  string `json:"followerEndCursor"`
	Viewer             string `json:"viewer"`
}

func main() {

	grpcServer := os.Getenv("GRPC_SERVER")
	log.Println("grpcServer:" + grpcServer)
	log.Println(grpcServer == "")

	pulsarBroker := os.Getenv("PULSAR_BROKER")

	grpcServer = "localhost:5560"

	if grpcServer == "" {
		grpcServer = "grpcServer:5560"
	}

	pulsarBroker = "192.168.3.75:6560"

	if pulsarBroker == "" {
		pulsarBroker = "pulsar:6650"
	}

	role := os.Getenv("LHG_ROLE")
	if role == "" {
		role = "worker"
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://" + pulsarBroker,
	})

	if err != nil {
		log.Println("create pulsar client fail:{}", err)
	}

	defer client.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "192.168.3.75:6389",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
	if err != nil {
		log.Println("ping redis fail:{}", err)
	}

	if role == "worker" {

		go func() {
			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				// topic lgh/viewer/{tokener}
				// Topic: "persistent://lgh/viewer/default",
				Topic: "persistent://smartoilets/tdb/default",
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

			// grpcClient := service.NewGithubLoaderClient(conn)
			// log.Println("begin grpc ==================")
			// resp, err := grpcClient.QueryFollow(context.Background(), &service.QueryFollowRequest{Login: "liangyuanpeng", Token: "22987b33dcb0e2a86b7c7557ef4f320edff9f44f", FollowingEndCursor: "", FollowerEndCursor: ""})
			// if err != nil {
			// 	log.Fatalf("could not queryFollow: %v", err)
			// }

			//两份数据，一份是保存当前数据查询进度 第二份是user follow info，

			// log.Printf("queryFollow.result: %s", resp.String())

			for {

				msg, err := consumer.Receive(context.Background())
				if err != nil {
					log.Fatal(err)
				}

				fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
					msg.ID(), string(msg.Payload()))

				// var queryFollowRequest &service.QueryFollowRequest
				var loadgithubMto LoadgithubMto

				// rdr := strings.NewReader(string(msg.Payload()))
				//  将json串rdr解码到结构体对象p1中

				jsonerr := json.Unmarshal([]byte(msg.Payload()), &loadgithubMto)
				if jsonerr != nil {
					log.Println("json parse fail:{}", jsonerr)
					continue
				}
				// err = json.NewDecoder(string(ext.Raw)).Decode(&conf)

				// 创建Waiter服务的客户端
				// t := service.NewGreeterClient(conn)

				grpcClient := service.NewGithubLoaderClient(conn)

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

				token, rerr := redisClient.Get("lgh_token_" + loadgithubMto.Viewer).Result()
				if rerr != nil {
					log.Println("get token fail:{}|{}", loadgithubMto, rerr)
					continue
				}

				log.Println("begin grpc ==================")
				resp, err := grpcClient.QueryFollow(context.Background(), &service.QueryFollowRequest{Login: loadgithubMto.Login, Token: token, FollowingEndCursor: loadgithubMto.FollowingEndCursor, FollowerEndCursor: loadgithubMto.FollowerEndCursor})
				// resp, err := grpcClient.QueryFollow(context.Background(), &service.QueryFollowRequest{Login: "liangyuanpeng", Token: "", FollowingEndCursor: "", FollowerEndCursor: ""})
				if err != nil {
					log.Fatalf("could not queryFollow: %v", err)
				}
				log.Printf("queryFollow.result: %s", resp.String())

			}
		}()

	}

	select {}

}
