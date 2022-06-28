package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wellscui/leader-election/leaderElection"
	"github.com/wellscui/leader-election/locks"
)

func TestLeaderelection(t *testing.T) {
	election:= leaderelection.Election{
		Name: "monitor_leader",
		Tenure: time.Second*5,
		Renewal: time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30) 
	defer cancel()
	db1, err := getMongoDB(ctx, "test")
	db2, err := getMongoDB(ctx, "test")
	require.NoError(t, err)
	locker1 := locks.NewMongoDBLocker(db1)
	locker2 := locks.NewMongoDBLocker(db2)

	candidate1:= leaderelection.NewManger("candidate1", []leaderelection.Election{election},  locker1)
	candidate2:= leaderelection.NewManger("candidate2", []leaderelection.Election{election},  locker2)
	go func(){
		candidate1.Start(ctx)
	}()

	go func(){
		candidate2.Start(ctx)
	}()

	ticker:=time.NewTicker(time.Millisecond*500)
	for {
		select {
		case <- ticker.C:
			fmt.Printf("candidate1: %v, candidate2: %v \n", candidate1.IsLeader("monitor_leader"), candidate2.IsLeader("monitor_leader"))
		case <- ctx.Done():
			return 
		}
	}
}
