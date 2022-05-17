package locks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	LockCollection    = "locks"
	FieldID           = "id"
	FieldOwner        = "owner"
	FieldResource     = "resource"
	FieldAcquiredTime = "acquiredTime"
	FieldUpdatedAt   = "updatedAt"
	FieldExpiry       = "expiry"
)

type MongoDBLocker struct {
	db *mongo.Database
}

func NewMongoDBLocker(db *mongo.Database) *MongoDBLocker {
	return &MongoDBLocker{
		db: db,
	}
}

func (locker *MongoDBLocker) Lock(ctx context.Context, resource string, options ...LockOption) (*Lock, error) {
	opts:=buildOptions(options)
	var err error
	var lock *Lock
	if opts.RetryOption!=nil {
		err = Retry(ctx, func() error {
			var lockErr error
			lock, lockErr = locker.innerLock(ctx, resource, opts)
			return lockErr
		}, RetryInterval(opts.RetryOption))
		return lock, err
	}
	return locker.innerLock(ctx, resource, opts)
}

func (locker *MongoDBLocker) innerLock(ctx context.Context, resource string, options *LockOptions) (*Lock, error) {
	filter := bson.D{{Key: FieldResource, Value: resource}}
	rs := locker.db.Collection(LockCollection).FindOne(ctx, filter)
	acquiredTime := time.Now().UTC()
	var expiry *time.Time
	if options.Expiry!=nil {
		deadLine := acquiredTime.Add(*options.Expiry)
		expiry = &deadLine
	}
	newLock := &Lock{
		ID:           primitive.NewObjectID().Hex(),
		Resource:     resource,
		Owner:        options.Owner,
		AcquiredTime: &acquiredTime,
		UpdatedAt:    &acquiredTime,
		Expiry:       expiry,
	}
	switch {
	case rs.Err() != nil && rs.Err() != mongo.ErrNoDocuments:
		return nil, fmt.Errorf("failed to lock: %w", rs.Err())
	case rs.Err() != nil && rs.Err() == mongo.ErrNoDocuments:
		_, err := locker.db.Collection(LockCollection).InsertOne(ctx, newLock)
		if err != nil {
			return nil, fmt.Errorf("failed to create the lock: %w", err)
		}
		return newLock, nil
	default:
		currentLock := &Lock{}
		err := rs.Decode(currentLock)
		if err != nil {
			return nil, fmt.Errorf("failed to decode lock: %w", rs.Err())
		}
		if !currentLock.Expired(options.RenewExpiry) {
			return currentLock, errors.New("resource has been locked by other owner")
		}

		fmt.Printf("currentLock: %+v \n", currentLock)
		updateFilter := bson.D{
			{Key: FieldID, Value: currentLock.ID},
			{Key: FieldResource, Value: currentLock.Resource},
			{Key: FieldOwner, Value: currentLock.Owner},
			{Key: FieldUpdatedAt, Value: currentLock.UpdatedAt},
			// {Key: FieldExpiry, Value: currentLock.Expiry},
		}
		fmt.Printf("update filter: %+v \n", updateFilter)
		newLock.ID = currentLock.ID
		updates := bson.M{"$set": bson.M{
			FieldOwner:        newLock.Owner,
			FieldAcquiredTime: newLock.AcquiredTime,
			FieldUpdatedAt:   newLock.UpdatedAt,
			FieldExpiry:       newLock.Expiry,
		}}
		updateResult, err := locker.db.Collection(LockCollection).UpdateOne(ctx, updateFilter, updates)
		if err != nil {
			return nil, fmt.Errorf("failed to update the lock: %w", err)
		}
		if updateResult.ModifiedCount == 0 {
			return nil, fmt.Errorf("resource has been acquired by other owner")
		}
		return newLock, nil
	}
}		

func (locker *MongoDBLocker) Unlock(ctx context.Context, lock *Lock) error {
	filter := bson.D{
		{Key: FieldID, Value: lock.ID},
		{Key: FieldResource, Value: lock.Resource},
		{Key: FieldOwner, Value: lock.Owner},
		{Key: FieldUpdatedAt, Value: lock.UpdatedAt},
		// {Key: FieldAcquiredTime, Value: currentLock.AcquiredTime},
	}
	rs, err := locker.db.Collection(LockCollection).DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete lock :%w", err)
	}
	if rs.DeletedCount == 0 {
		return fmt.Errorf("lock is changed")
	}
	return nil
}

func (locker *MongoDBLocker) Renew(ctx context.Context, lock *Lock) error {
	updateTime := time.Now().UTC()
	filter := bson.D{
		{Key: FieldID, Value: lock.ID},
		{Key: FieldResource, Value: lock.Resource},
		{Key: FieldOwner, Value: lock.Owner},
		{Key: FieldUpdatedAt, Value: lock.UpdatedAt},
		// {Key: FieldAcquiredTime, Value: currentLock.AcquiredTime},
	}
	updates := bson.M{"$set": bson.M{
		FieldUpdatedAt: &updateTime,
	}}
	rs, err := locker.db.Collection(LockCollection).UpdateOne(ctx, filter, updates)
	if err != nil {
		return fmt.Errorf("failed to update the lock: %w", err)
	}
	if rs.ModifiedCount == 0 {
		return fmt.Errorf("resource has been acquired by other owner")
	}
	lock.UpdatedAt = &updateTime
	return nil
}
