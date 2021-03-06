package locks

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
)

const (
	LockCollection    = "locks"
	FieldID           = "id"
	FieldOwner        = "owner"
	FieldResource     = "resource"
	FieldAcquiredTime = "acquiredTime"
	FieldUpdatedAt    = "updatedAt"
	FieldExpiry       = "expiry"
	FieldPriority     = "priority"
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
	opts := buildOptions(options)
	var err error
	var lock *Lock
	if opts.RetryOption != nil {
		err = Retry(ctx, func() error {
			var lockErr error
			lock, lockErr = locker.innerLock(ctx, resource, opts)
			return lockErr
		}, RetryInterval(opts.RetryOption))
		return lock, err
	}
	return locker.innerLock(ctx, resource, opts)
}

func conditionalUpdate(fieldName string, cond, updateValue interface{}) bson.E {
	return bson.E{Key: fieldName, Value: bson.M{
		"$cond": bson.M{
			"if":   cond,
			"then": updateValue,
			"else": "$" + fieldName,
		},
	}}
}

func (locker *MongoDBLocker) innerLock(ctx context.Context, resource string, options *lockOptions) (*Lock, error) {
	filter := bson.D{{Key: FieldResource, Value: resource}}
	now := time.Now().UTC().Truncate(time.Millisecond)

	updateConds := []bson.M{{"$lt": []interface{}{"$" + FieldExpiry, &now}}}
	if options.Priority != nil {
		updateConds = append(updateConds, bson.M{"$lt": []interface{}{"$" + FieldPriority, options.Priority}})
	}
	if options.UpdateExpiry!= nil {
		updateExpiry := now.Add(-*options.UpdateExpiry)
		updateConds = append(updateConds, bson.M{"$lt": []interface{}{"$" + FieldUpdatedAt, updateExpiry}})
	}

	updateCond := bson.M{ "$or": updateConds}

	updateTime := &now
	acquiredTime := &now
	var expiry *time.Time
	if options.Expiry != nil {
		deadLine := acquiredTime.Add(*options.Expiry).UTC()
		expiry = &deadLine
	}

	updates := bson.A{bson.M{
		"$set": bson.D{
			{Key: FieldResource, Value: resource},
			conditionalUpdate(FieldOwner, updateCond, options.Owner),
			conditionalUpdate(FieldUpdatedAt, updateCond, updateTime),
			conditionalUpdate(FieldAcquiredTime, updateCond, acquiredTime),
			conditionalUpdate(FieldExpiry, updateCond, expiry),
			conditionalUpdate(FieldPriority, updateCond, options.Priority),
		},
	}}
	upsert := true
	returnDocument := mongoOptions.After

	rs := locker.db.Collection(LockCollection).FindOneAndUpdate(ctx, filter, updates, &mongoOptions.FindOneAndUpdateOptions{
		Upsert:         &upsert,
		ReturnDocument: &returnDocument,
	})

	if rs.Err() != nil {
		return nil, fmt.Errorf("failed to acquire the lock: %w", rs.Err())
	}
	lock := &Lock{}
	if err := rs.Decode(&lock); err != nil {
		return nil, fmt.Errorf("failed to decode the lock: %w", err)
	}

	if lock.Owner != options.Owner {
		return lock, fmt.Errorf("resource has been acquired by other owner")
	}
	return lock, nil
}

func (locker *MongoDBLocker) Unlock(ctx context.Context, lock *Lock) error {
	filter := bson.D{
		{Key: FieldResource, Value: lock.Resource},
		{Key: FieldOwner, Value: lock.Owner},
		{Key: FieldUpdatedAt, Value: lock.UpdatedAt},
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
	updateTime := time.Now().UTC().Truncate(time.Millisecond)
	filter := bson.D{
		{Key: FieldResource, Value: lock.Resource},
		{Key: FieldOwner, Value: lock.Owner},
		{Key: FieldUpdatedAt, Value: lock.UpdatedAt},
	}
	updates := bson.M{"$set": bson.M{
		FieldExpiry:    lock.Expiry,
		FieldUpdatedAt: &updateTime,
		FieldPriority: lock.Priority,
	}}
	rs, err := locker.db.Collection(LockCollection).UpdateOne(ctx, filter, updates)
	if err != nil {
		return fmt.Errorf("failed to renew the lock: %w", err)
	}
	if rs.ModifiedCount == 0 {
		return fmt.Errorf("resource has been acquired by other owner")
	}
	lock.UpdatedAt = &updateTime
	return nil
}

func (locker *MongoDBLocker) Find(ctx context.Context, resource string) (*Lock, error) {
	filter := bson.D{
		{Key: FieldResource, Value: resource},
	}
	rs:=locker.db.Collection(LockCollection).FindOne(ctx, filter)
	if rs.Err()!=nil {
		return nil, fmt.Errorf("failed to find lock: %w", rs.Err())
	}
	lock := &Lock{}
	if err := rs.Decode(&lock); err != nil {
		return nil, fmt.Errorf("failed to decode the lock: %w", err)
	}
	return lock, nil
}
