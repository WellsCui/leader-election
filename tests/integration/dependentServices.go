package integration

import (
	"context"
	"fmt"
	"os"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo"
)



func startMongo(ctx context.Context, withInDocker bool) error {
	mongoSvc := NewMongoService(withInDocker)
	instance, err := mongoSvc.Start()
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		mongoSvc.Stop()
	}()

	if withInDocker {
		err = os.Setenv("MONGODB_URI", "mongodb://"+instance.DockerHost)
	} else {
		err = os.Setenv("MONGODB_URI", "mongodb://"+instance.Host)
	}

	if err != nil {
		return fmt.Errorf("failed to set MONGO_URI env vars: %v", err)
	}

	return err
}

func getMongoDB(ctx context.Context, db string) (*mongo.Database, error) {
	uri:= os.Getenv("MONGODB_URI")
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err!=nil {
		return nil, err
	}
	return mongoClient.Database(db), nil
}
