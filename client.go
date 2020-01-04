package main

import (
	"encoding/json"
	"fmt"
	"loadgithub/service"
	"log"
	"os"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis/v7"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LoadgithubMto struct {
	Login              string `json:"login"`
	FollowingEndCursor string `json:"followingEndCursor"`
	FollowerEndCursor  string `json:"followerEndCursor"`
	Viewer             string `json:"viewer"`
}

// login FollowingEndCursor FollowerEndCursor order
type SaveBaseDto struct {
	Login              string `json:"login"`
	FollowingEndCursor string `json:"followingEndCursor"`
	FollowerEndCursor  string `json:"followerEndCursor"`
	Order              int    `json:"order"`
}

func main() {

	log.Println("=================================")

	grpcServer := os.Getenv("GRPC_SERVER")
	log.Println("grpcServer:" + grpcServer)
	log.Println(grpcServer == "")

	pulsarBroker := os.Getenv("PULSAR_BROKER")

	if grpcServer == "" {
		grpcServer = "grpcServer:5560"
	}

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

	ctx := context.Background()
	// mongoCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6389",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
	if err != nil {
		log.Println("ping redis fail:{}", err)
	}

	// 设置客户端连接配置
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// 连接到MongoDB
	mongoClient, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal("==============connect mongodb fail:{}", err)
	}

	// 检查连接
	err = mongoClient.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal("============mongo ping fail{}", err)
	}
	fmt.Println("Connected to MongoDB!")

	collection := mongoClient.Database("lgh").Collection("workerQueue")
	// res, err := collection.InsertOne(mongoCtx, bson.M{"name": "pi", "value": 3.14159})
	// if err != nil {
	// 	log.Println("===============insert to mongo fail:{}", err)
	// }
	// id := res.InsertedID

	// log.Println("insert.mongodb.data.success:{}", id)

	if role == "worker" {

		go func() {
			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				// topic lgh/viewer/{tokener}
				Topic: "persistent://public/default/default",
				// Topic: "persistent://lgh/viewer/default",
				// Topic: "persistent://smartoilets/tdb/default",
				// message base
				//1. token  ->  lgh_token_default  --> redis
				//2. login
				//3.followingEndCursor
				//4.followerEndCursor

				//extra
				//1.following limit
				//2.follower limit
				SubscriptionName: "my-sub3",
				Type:             pulsar.Shared,
			})

			if err != nil {
				log.Fatal("pulsar.consume.worker.fail:{}", err)
				return
			}

			defer consumer.Close()

			// 建立连接到gRPC服务
			conn, err := grpc.Dial(grpcServer, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			// 函数结束时关闭连接
			defer conn.Close()

			producer, errp := client.CreateProducer(pulsar.ProducerOptions{
				Topic: "persistent://public/default/mongo-base",
			})
			if errp != nil {
				log.Fatal(errp)
			}

			defer producer.Close()

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

				//TODO viewer 作为topic11

				token, rerr := redisClient.Get("lgh_token").Result()
				if rerr != nil {
					log.Println("get token fail:{}|{}", loadgithubMto, rerr)
					continue
				}

				log.Println("begin grpc ==================")
				resp, err := grpcClient.QueryFollow(context.Background(), &service.QueryFollowRequest{Login: loadgithubMto.Login, Token: token, FollowingEndCursor: loadgithubMto.FollowingEndCursor, FollowerEndCursor: loadgithubMto.FollowerEndCursor})
				// resp, err := grpcClient.QueryFollow(context.Background(), &service.QueryFollowRequest{Login: "liangyuanpeng", Token: "", FollowingEndCursor: "", FollowerEndCursor: ""})
				if err != nil {
					log.Fatalf("could not queryFollow: %v", err)
					continue
				}
				log.Printf("queryFollow.result: %s", resp.String())
				//TODO insert data topic
				//persistent://lgh/store/{storedb}  default mongodb
				//TODO worker优化关闭， 考虑消费了topic没有执行行为

				//login FollowingEndCursor FollowerEndCursor order

				//login name updateAt company email

				//关系表  login_cursor{login_followingEndCursor_followerEndCursor} following follower

				for _, following := range resp.GetData().GetUser().GetFollowing().GetNodes() {
					var saveBaseDto SaveBaseDto
					saveBaseDto.Login = following.Login
					saveBaseDto.FollowingEndCursor = ""
					saveBaseDto.FollowerEndCursor = ""

					jsonstr, err := json.Marshal(&saveBaseDto)
					if err != nil {
						fmt.Println(err.Error())
						continue
					}

					//msgId
					if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
						Payload: []byte(jsonstr),
					}); err != nil {
						log.Printf("pulsar.send.fail: %s", err)
					}
				}

				for _, following := range resp.GetData().GetUser().GetFollowers().GetNodes() {
					var saveBaseDto SaveBaseDto
					saveBaseDto.Login = following.Login
					saveBaseDto.FollowingEndCursor = ""
					saveBaseDto.FollowerEndCursor = ""

					jsonstr, err := json.Marshal(&saveBaseDto)
					if err != nil {
						fmt.Println(err.Error())
						continue
					}

					//msgId
					if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
						Payload: []byte(jsonstr),
					}); err != nil {
						log.Printf("pulsar.send.fail: %s", err)
					}
				}

				var saveBaseDto SaveBaseDto
				saveBaseDto.Login = loadgithubMto.Login
				saveBaseDto.FollowingEndCursor = resp.GetData().GetUser().GetFollowing().GetPageInfo().GetEndCursor()
				saveBaseDto.FollowerEndCursor = resp.GetData().GetUser().GetFollowers().GetPageInfo().GetEndCursor()

				jsonstr, err := json.Marshal(&saveBaseDto)
				if err != nil {
					fmt.Println(err.Error())
					continue
				}

				//msgId
				if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
					Payload: []byte(jsonstr),
				}); err != nil {
					log.Printf("pulsar.send.fail: %s", err)
				}
				//确认消费
				consumer.Ack(msg)
			}
		}()

	}

	role = "saver"
	log.Println("role == saver:{}", (role == "saver"))

	if role == "saver" {

		go func() {
			log.Println("begin saver==========================")
			//监听pulsar 保存数据到mongodb
			consumer, err := client.Subscribe(pulsar.ConsumerOptions{
				// topic lgh/viewer/{tokener}
				Topic:            "persistent://public/default/mongo-base",
				SubscriptionName: "my-sub3",
				Type:             pulsar.Shared,
			})
			if err != nil {
				log.Fatal("pulsar.consume.worker.fail:{}", err)
				return
			}

			defer consumer.Close()

			for {

				mongoCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)

				msg, err := consumer.Receive(context.Background())
				if err != nil {
					log.Fatal(err)
				}

				fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
					msg.ID(), string(msg.Payload()))

				var saveBaseDto SaveBaseDto

				jsonerr := json.Unmarshal([]byte(msg.Payload()), &saveBaseDto)
				if jsonerr != nil {
					log.Println("json parse fail:{}", jsonerr)
					continue
				}

				res, err := collection.InsertOne(mongoCtx, saveBaseDto)
				if err != nil {
					if strings.Index(err.Error(), "dup key") > 0 {
						//重复唯一索引，可以确认消费消息
						log.Println("mongodb.dup key:{}", err)
					} else {
						log.Println("===============insert to mongo fail:{}", err)
						continue
					}
				} else {
					id := res.InsertedID
					log.Println("insert.mongodb.success:{}", id)
				}

				//确认消费
				consumer.Ack(msg)

			}
		}()
	}

	if role == "tasker" {
		//定时load mongodb task放入redis 队列
	}

	if role == "committer" {
		//从redis 队列拿数据 放入pulsar
		//并且将login放入task记录，且加入过期时间，在没过期前不再执行相关task
	}

	select {}

}
