package main

import (
	"context"
	"fmt"
	"log"
	"os"

	dotenv "github.com/joho/godotenv"
	cron "github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	type ResultCollection struct {
		ID primitive.ObjectID `bson:"_id,omitempty"`
	}

	projectStage := bson.D{
		{Key: "$project", Value: bson.D{
			{Key: "isInactive", Value: bson.D{
				{Key: "$lte", Value: bson.A{
					"$activityThreshold", bson.D{
						{Key: "$subtract", Value: bson.A{"$$NOW", "$lastseen"}},
					}},
				}},
			}},
		},
	}
	matchStage := bson.D{{Key: "$match", Value: bson.D{{Key: "isInactive", Value: true}}}}

	showLoadedStructCursor, err := presenceCollection.Aggregate(ctx, mongo.Pipeline{projectStage, matchStage})
	if err != nil {
		panic(err)
	}
	var loadedStruct []ResultCollection
	if err = showLoadedStructCursor.All(ctx, &loadedStruct); err != nil {
		panic(err)
	}

	var idSlice []primitive.ObjectID
	for _, item := range loadedStruct {
		idSlice = append(idSlice, item.ID)
	}

	presenceCollection.UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": idSlice}},
		bson.M{"$set": bson.M{"activity": 777}},
	)
	fmt.Println(idSlice)
	return true
}

func activity_cron(presenceCollection *mongo.Collection, ctx context.Context) {
	duration := os.Getenv("DURATION")
	repeats := fmt.Sprintf("@every %ss", duration)

	c := cron.New()
	c.AddFunc(repeats, func() {
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
