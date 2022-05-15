package integration

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"

)

var (
	useDocker = flag.Bool("usedocker", false, "whether or not to use container ip addresses for the test")
)

func TestMain(m *testing.M) {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	err := startMongo(ctx, false)

	statusCode := -1
	if err == nil {
		// config.LoadConfigFromEnv(ctx)
		statusCode = m.Run()
	} else {
		log.Println("failed to start dependent services: ", err)
	}

	if statusCode != 0 {
		log.Println("Integration test suite returned invalid status code: ", statusCode)
	} else {
		log.Println("\n\ntests completed.")
	}
	cancel()
	os.Exit(statusCode)
}
