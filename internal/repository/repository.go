package repository

import (
	"context"
	"errors"
	"fmt"

	uuid "github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

var (
	ErrDuplicateRecord = errors.New("error duplicate record")
	ErrInsertingRecord = errors.New("error inserting record")
	ErrFindingRecord   = errors.New("error finding record")
)

type Repository struct {
	collection *mongo.Collection
	ttl        int32
}

type Record struct {
	ID        uuid.UUID          `bson:"id"`
	Timestamp primitive.DateTime `bson:"timestamp"`
}

func NewRepository(ctx context.Context, collection *mongo.Collection, ttl int32) (Repository, error) {
	r := Repository{
		collection: collection,
		ttl:        ttl,
	}
	if err := r.ensureIndexes(ctx); err != nil {
		return Repository{}, err
	}

	return r, nil
}

func (r Repository) ensureIndexes(ctx context.Context) error {
	_, err := r.collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bsonx.Doc{{Key: "id", Value: bsonx.Int64(1)}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    bsonx.Doc{{Key: "timestamp", Value: bsonx.Int64(1)}},
			Options: options.Index().SetExpireAfterSeconds(r.ttl),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (r Repository) StoreRecord(ctx context.Context, record Record) error {
	_, err := r.collection.InsertOne(ctx, record)
	if err != nil {
		if IsDuplicateKeyException(err) {
			return fmt.Errorf("%w: %v", ErrDuplicateRecord, err)
		}
		return fmt.Errorf("%w: %v", ErrInsertingRecord, err)
	}

	return nil
}

func (r Repository) GetRecord(ctx context.Context, recordID uuid.UUID) (Record, error) {
	filter := bson.D{primitive.E{Key: "id", Value: recordID}}
	var record Record
	if err := r.collection.FindOne(ctx, filter).Decode(&record); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrFindingRecord, err)
	}

	return record, nil
}

func IsDuplicateKeyException(err error) bool {
	var e mongo.WriteException
	if errors.As(err, &e) {
		for _, we := range e.WriteErrors {
			if we.Code == 11000 {
				return true
			}
		}
	}
	return false
}
