package main

import (
	"encoding/json"
	"fmt"
	"loadgithub/service"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis/v7"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"go.mongodb.org/mongo-driver/bson"
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
type SaveWorkerDto struct {
	Login              string `json:"login"`
	FollowingEndCursor string `json:"followingEndCursor"`
	FollowerEndCursor  string `json:"followerEndCursor"`
	Order              int    `json:"order"`
}

// login name updateAt email company
type SaveBaseDto struct {
	Login    string `json:"login"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Company  string `json:"company"`
	UpdateAt string `json:"updateAt"`
}

func main() {

	rand.Seed(time.Now().UnixNano())

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

	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://" + pulsarBroker,
	})

	if err != nil {
		log.Println("create pulsar client fail:{}", err)
	}

	defer pulsarClient.Close()

	redisHost := os.Getenv("REDIS_HOST")

	if redisHost == "" {
		redisHost = "redis:6379"
	}

	mongoHost := os.Getenv("MONGO_HOST")

	if mongoHost == "" {
		mongoHost = "mongo:27017"
	}

	token := os.Getenv("TOKEN")
	if token == "" {
		log.Println("token must be init")
		panic(-1)
	}

	ctx := context.Background()
	// mongoCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisHost,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := redisClient.Ping().Result()
	fmt.Println(pong, err)
	if err != nil {
		log.Println("ping redis fail:{}", err)
		panic(-1)
	}

	// 设置客户端连接配置
	clientOptions := options.Client().ApplyURI("mongodb://" + mongoHost)

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
	mongoBaseCollection := mongoClient.Database("lgh").Collection("base")
	// res, err := collection.InsertOne(mongoCtx, bson.M{"name": "pi", "value": 3.14159})
	// if err != nil {
	// 	log.Println("===============insert to mongo fail:{}", err)
	// }
	// id := res.InsertedID

	// log.Println("insert.mongodb.data.success:{}", id)

	if role == "worker" {

		go func() {
			consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
				// topic lgh/viewer/{tokener}
				// Topic:         "persistent://public/default/default",
				// TopicsPattern: "persistent://public/default/viewer-*",
				TopicsPattern: "persistent://public/default/viewer",
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

			producer, errp := pulsarClient.CreateProducer(pulsar.ProducerOptions{
				Topic: "persistent://public/default/mongo-worker",
			})
			if errp != nil {
				log.Fatal(errp)
			}

			defer producer.Close()

			baseProducer, errp := pulsarClient.CreateProducer(pulsar.ProducerOptions{
				Topic: "persistent://public/default/mongo-base",
			})
			if errp != nil {
				log.Fatal(errp)
			}

			defer baseProducer.Close()

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

				// fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
				// msg.ID(), string(msg.Payload()))

				// var queryFollowRequest &service.QueryFollowRequest
				var loadgithubMto LoadgithubMto

				// rdr := strings.NewReader(string(msg.Payload()))
				//  将json串rdr解码到结构体对象p1中

				jsonerr := json.Unmarshal([]byte(msg.Payload()), &loadgithubMto)
				if jsonerr != nil {
					log.Println("json parse fail:{}", jsonerr)
					continue
				}

				//TODO 考虑只有一个end情况
				if loadgithubMto.FollowingEndCursor == "end" && loadgithubMto.FollowerEndCursor == "end" {
					consumer.Ack(msg)
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

				// token, rerr := redisClient.Get("lgh_token").Result()
				// if rerr != nil {
				// 	log.Println("get token fail:{}|{}", loadgithubMto, rerr)
				// 	continue
				// }

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

				//TODO 存储follow关系

				for _, following := range resp.GetData().GetUser().GetFollowing().GetNodes() {
					var saveWorkerDto SaveWorkerDto
					saveWorkerDto.Login = following.Login
					saveWorkerDto.FollowingEndCursor = ""
					saveWorkerDto.FollowerEndCursor = ""

					jsonstr, err := json.Marshal(&saveWorkerDto)
					if err != nil {
						log.Println("parse.json.fail.worker:{}", err.Error())
						continue
					}

					//msgId
					if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
						Payload: []byte(jsonstr),
					}); err != nil {
						log.Printf("pulsar.send.fail: %s", err)
					}

					var saveBaseDto SaveBaseDto
					saveBaseDto.Login = following.Login
					saveBaseDto.Name = following.Name
					saveBaseDto.Email = following.Email
					saveBaseDto.Company = following.Company
					saveBaseDto.UpdateAt = following.UpdatedAt

					jsonstr2, err2 := json.Marshal(&saveBaseDto)
					if err2 != nil {
						log.Println("parse.json.fail.worker:{}", err.Error())
						continue
					}

					//msgId
					if _, err := baseProducer.Send(ctx, &pulsar.ProducerMessage{
						Payload: []byte(jsonstr2),
					}); err != nil {
						log.Printf("pulsar.send.base.fail: %s", err)
					}

				}

				for _, following := range resp.GetData().GetUser().GetFollowers().GetNodes() {
					var saveWorkerDto SaveWorkerDto
					saveWorkerDto.Login = following.Login
					saveWorkerDto.FollowingEndCursor = ""
					saveWorkerDto.FollowerEndCursor = ""

					jsonstr, err := json.Marshal(&saveWorkerDto)
					if err != nil {
						log.Println("parse.json.fail.worker:{}", err.Error())
						continue
					}

					//msgId
					if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
						Payload: []byte(jsonstr),
					}); err != nil {
						log.Printf("pulsar.send.fail: %s", err)
					}

					var saveBaseDto SaveBaseDto
					saveBaseDto.Login = following.Login
					saveBaseDto.Name = following.Name
					saveBaseDto.Email = following.Email
					saveBaseDto.Company = following.Company
					saveBaseDto.UpdateAt = following.UpdatedAt

					jsonstr2, err2 := json.Marshal(&saveBaseDto)
					if err2 != nil {
						log.Println("parse.json.fail.worker:{}", err.Error())
						continue
					}

					//msgId
					if _, err := baseProducer.Send(ctx, &pulsar.ProducerMessage{
						Payload: []byte(jsonstr2),
					}); err != nil {
						log.Printf("pulsar.send.base.fail: %s", err)
					}
				}

				var saveWorkerDto SaveWorkerDto
				saveWorkerDto.Login = loadgithubMto.Login
				saveWorkerDto.FollowingEndCursor = resp.GetData().GetUser().GetFollowing().GetPageInfo().GetEndCursor()
				saveWorkerDto.FollowerEndCursor = resp.GetData().GetUser().GetFollowers().GetPageInfo().GetEndCursor()

				if !resp.GetData().GetUser().GetFollowing().GetPageInfo().HasNextPage {
					saveWorkerDto.FollowingEndCursor = "end"
				}

				if !resp.GetData().GetUser().GetFollowers().GetPageInfo().HasNextPage {
					saveWorkerDto.FollowerEndCursor = "end"
				}

				jsonstr, err := json.Marshal(&saveWorkerDto)
				if err != nil {
					log.Println("parse.json.fail.worker:{}", err.Error())
					continue
				}

				//msgId
				if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
					Payload: []byte(jsonstr),
				}); err != nil {
					log.Printf("pulsar.send.fail: %s", err)
				}

				// var saveBaseDto SaveBaseDto
				// saveBaseDto.Login = loadgithubMto.Login
				// saveBaseDto.Name = following.Name
				// saveBaseDto.Email = following.Email
				// saveBaseDto.Company = following.Company
				// saveBaseDto.UpdateAt = following.UpdatedAt

				// jsonstr2, err2 := json.Marshal(&saveBaseDto)
				// if err2 != nil {
				// 	log.Println("parse.json.fail.worker:{}", err.Error())
				// 	continue
				// }

				// //msgId
				// if _, err := baseProducer.Send(ctx, &pulsar.ProducerMessage{
				// 	Payload: []byte(jsonstr2),
				// }); err != nil {
				// 	log.Printf("pulsar.send.base.fail: %s", err)
				// }

				//确认消费
				consumer.Ack(msg)
			}
		}()

	}

	role = "saver"
	log.Println("role == saver:{}", (role == "saver"))

	if role == "saver" {

		//listener worker msg
		go func() {
			log.Println("begin saver==========================")
			//监听pulsar 保存数据到mongodb
			consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
				// topic lgh/viewer/{tokener}
				Topic:            "persistent://public/default/mongo-worker",
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

				// fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
				// msg.ID(), string(msg.Payload()))

				var saveBaseDto SaveWorkerDto

				jsonerr := json.Unmarshal([]byte(msg.Payload()), &saveBaseDto)
				if jsonerr != nil {
					log.Println("json parse fail:{}", jsonerr)
					continue
				}

				exist, rerr := redisClient.Get("lgh_exist_" + saveBaseDto.Login).Result()
				if rerr != nil {
					if strings.Index(rerr.Error(), "nil") > 0 {
						exist = ""
					} else {
						log.Println("get lgh_exist fail:{}", saveBaseDto.Login, rerr)
						continue
					}
				}

				// log.Println("============exist:{}", exist)

				if exist == "" {
					res, err := collection.InsertOne(mongoCtx, saveBaseDto)
					if err != nil {
						if strings.Index(err.Error(), "dup key") > 0 {
							//重复唯一索引，可以确认消费消息  需要更新数据
							log.Println("mongodb.dup key:{}", err)
						} else {
							log.Println("===============insert to mongo worker fail:{}", err)
							continue
						}
					} else {
						id := res.InsertedID
						log.Println("insert.mongodb.worker.success:{}", id)
						//TODO 插入成功后写到缓存，如果缓存有则是需要更新
						err := redisClient.Set("lgh_exist_"+saveBaseDto.Login, "1", 0).Err()
						if err != nil {
							log.Println("redis.set.key.fail:{}", err)
						}
					}
				} else {
					//更新
					//结束是end标识，""只标识开始，redis有数据则标识已经开始 无效消息，丢弃
					if saveBaseDto.FollowingEndCursor != "" || saveBaseDto.FollowerEndCursor != "" {
						filter := bson.M{"login": saveBaseDto.Login}
						data := bson.M{"$set": bson.M{"followingEndCursor": saveBaseDto.FollowingEndCursor, "followierEndCursor": saveBaseDto.FollowerEndCursor}}
						collection.UpdateOne(mongoCtx, filter, data)
					}
				}

				//确认消费
				consumer.Ack(msg)
			}
		}()

		//listener base msg
		go func() {
			log.Println("begin saver==========================")
			//监听pulsar 保存数据到mongodb
			consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
				// topic lgh/viewer/{tokener}
				Topic:            "persistent://public/default/mongo-base",
				SubscriptionName: "my-sub3",
				Type:             pulsar.Shared,
			})
			if err != nil {
				log.Fatal("pulsar.consume.base.fail:{}", err)
				return
			}

			defer consumer.Close()

			for {

				mongoCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)

				msg, err := consumer.Receive(context.Background())
				if err != nil {
					log.Fatal(err)
				}

				// fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
				// msg.ID(), string(msg.Payload()))

				var saveBaseDto SaveBaseDto

				jsonerr := json.Unmarshal([]byte(msg.Payload()), &saveBaseDto)
				if jsonerr != nil {
					log.Println("json parse fail:{}", jsonerr)
					continue
				}

				res, err := mongoBaseCollection.InsertOne(mongoCtx, saveBaseDto)
				if err != nil {
					if strings.Index(err.Error(), "dup key") > 0 {
						log.Println("mongodb.dup key:{}", err)
					} else {
						log.Println("===============insert to mongo base fail:{}", err)
						continue
					}
				} else {
					id := res.InsertedID
					log.Println("insert.mongodb.base.success:{}", id)
				}
				//确认消费
				consumer.Ack(msg)
			}
		}()
	}

	role = "tasker"

	if role == "tasker" {

		//定时load mongodb task放入redis 队列
		var ch chan int
		//定时任务
		ticker := time.NewTicker(time.Second * 300)
		go func() {

			for range ticker.C {

				randomOrder := rand.Intn(100)

				// 按类型、状态筛选
				filter := bson.M{
					"followingEndCursor": bson.M{"$ne": "end"},
					"followerEndCursor":  bson.M{"$ne": "end"},
					"order":              bson.M{"$lt": randomOrder},
				}

				ctx = context.Background()
				// filter 过滤条件
				// options.Find() 返回一个查找选项实例
				// SetSort(bson.M{sort: -1}) 按照字段排序，-1表示降序，这里的sort可以为"title"或"type"等数据表的字段
				// SetSkip(skip) 设置跳过多少条记录
				// SetLimit(limit) 设置最多选择多少条记录

				//TODO end的数据还能查出来  需要fix
				cursor, err := collection.Find(context.Background(), filter, options.Find().SetSort(bson.M{"order": -1}).SetSkip(0).SetLimit(100))
				if err != nil {
					log.Println("tasker.select.mongodb.fail:{}", err)
				}

				var workers []SaveWorkerDto

				defer cursor.Close(ctx)
				for cursor.Next(ctx) {
					worker := SaveWorkerDto{}
					// task := TaskSchema{}
					err = cursor.Decode(&worker)
					if err != nil {
						return
					}
					workers = append(workers, worker)
				}

				for _, item := range workers {
					// var loadgithubMto LoadgithubMto
					// loadgithubMto.Viewer = "liangyuanpeng"
					// loadgithubMto.Login = item.Login
					// loadgithubMto.FollowingEndCursor = item.FollowingEndCursor
					// loadgithubMto.FollowerEndCursor = item.FollowerEndCursor
					// jsonstr, err := json.Marshal(&loadgithubMto)
					// if err != nil {
					// 	log.Println("parse.json.fail.worker:{}", err.Error())
					// 	continue
					// }
					// //msgId
					// if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
					// 	Payload: []byte(jsonstr),
					// }); err != nil {
					// 	log.Printf("pulsar.send.fail: %s", err)
					// }

					//放入redis 队列
					redisClient.SAdd("lgh_task", item.Login)

					log.Println("===============work:{}", item)
				}

				fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
			}
			ch <- 1
		}()
		// <-ch
	}

	log.Println("hello committer")

	role = "committer"

	if role == "committer" {
		//定时从redis 队列拿数据 放入pulsar
		//并且将login放入task记录，且加入过期时间，在没过期前不再执行相关task
		var ch chan int
		ticker := time.NewTicker(time.Second * 120)
		go func() {

			producer, errp := pulsarClient.CreateProducer(pulsar.ProducerOptions{
				Topic: "persistent://public/default/viewer",
			})
			if errp != nil {
				log.Fatal(errp)
			}

			for range ticker.C {

				for i := 0; i < 5; i++ {
					task, err := redisClient.SPop("lgh_task").Result()
					log.Println("=================committer.task:{}", task)
					if err != nil {
						log.Println("lgh_task.rpop.fail:{}", err)
					}
					if task == "" {
						break
					}

					var saveWorkerDto SaveWorkerDto
					filter := bson.M{"login": task}
					collection.FindOne(context.Background(), filter).Decode(&saveWorkerDto)
					log.Println("==============saveWorkerDto:{}=========", saveWorkerDto)

					var loadgithubMto LoadgithubMto

					loadgithubMto.Viewer = "liangyuanpeng"
					loadgithubMto.Login = saveWorkerDto.Login
					loadgithubMto.FollowingEndCursor = saveWorkerDto.FollowingEndCursor
					loadgithubMto.FollowerEndCursor = saveWorkerDto.FollowerEndCursor
					jsonstr, err := json.Marshal(&loadgithubMto)
					if err != nil {
						log.Println("parse.json.fail.worker:{}", err.Error())
						continue
					}
					// msgId
					if _, err := producer.Send(ctx, &pulsar.ProducerMessage{
						Payload: []byte(jsonstr),
					}); err != nil {
						log.Printf("pulsar.send.fail: %s", err)
					}
				}

				fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
			}
			ch <- 1
		}()
		<-ch
	}

	select {}

}
