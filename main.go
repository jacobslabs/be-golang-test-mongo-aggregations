package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dotenv "github.com/joho/godotenv"
	cron "github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	dotenv.Load()
	var MONGO_URI = os.Getenv("MONGO_URI")

	client, ctx := mongo_init(MONGO_URI)
	defer client.Disconnect(ctx)

	database := client.Database("golang_aggregation_demo")
	presenceCollection := database.Collection("presence")

	go activity_cron(presenceCollection, ctx)
	select {}
}

func exec_aggregation(presenceCollection *mongo.Collection, ctx context.Context) bool {
	type PodcastEpisode struct {
		ActivityThreshold int64     `bson:"activityThreshold,omitempty"`
		LastSeen          time.Time `bson:"lastseen,omitempty"`
		IsInactive        bool      `bson:"isInactive,omitempty"`
	}

	projectStage := bson.D{
		{"$project", bson.D{
			{"activityThreshold", 1},
			{"activity", 1},
			{"lastseen", 1},
			{"isInactive", bson.D{
				{"$lte", bson.A{
					"$activityThreshold", bson.D{
						{"$subtract", bson.A{"$$NOW", "$lastseen"}},
					}},
				}},
			}},
		}}
	matchStage := bson.D{{"$match", bson.D{{"isInactive", true}}}}
	setStage := bson.D{{"$set", bson.D{{"activity", 666}}}}

	showLoadedStructCursor, err := presenceCollection.Aggregate(ctx, mongo.Pipeline{projectStage, matchStage, setStage})
	if err != nil {
		panic(err)
	}
	var showsLoadedStruct []PodcastEpisode
	if err = showLoadedStructCursor.All(ctx, &showsLoadedStruct); err != nil {
		panic(err)
	}
	fmt.Println(showsLoadedStruct)
	return true
}

func activity_cron(presenceCollection *mongo.Collection, ctx context.Context) {
	c := cron.New()
	c.AddFunc("@every 10s", func() {
		exec_aggregation(presenceCollection, ctx)
	})
	c.Start()
}

func mongo_init(mongo_uri string) (*mongo.Client, context.Context) {
	client, err := mongo.NewClient(options.Client().ApplyURI(mongo_uri))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	return client, ctx
}
