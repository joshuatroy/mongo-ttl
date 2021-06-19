package repository

import (
	"context"
	"errors"
	"fmt"

	uuid "github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"

	"mongo-ttl/internal/domain"
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

// NewRepository func
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
	index := mongo.IndexModel{
		Keys: bsonx.Doc{
			{
				Key:   "timestamp",
				Value: bsonx.Int64(1),
			},
		},
	}
	_, err := r.collection.Indexes().CreateOne(ctx, index)
	if err != nil {
		return err
	}

	return nil
}

func (r Repository) StoreRecord(ctx context.Context, record domain.Record) error {
	_, err := r.collection.InsertOne(ctx, Record{
		ID:        record.ID,
		Timestamp: primitive.NewDateTimeFromTime(record.Timestamp),
	})
	if err != nil {
		if IsDuplicateKeyException(err) {
			return fmt.Errorf("%w: %v", ErrDuplicateRecord, err)
		}
		return fmt.Errorf("%w: %v", ErrInsertingRecord, err)
	}

	return nil
}

func (r Repository) GetRecord(ctx context.Context, recordID uuid.UUID) (domain.Record, error) {
	filter := bson.D{primitive.E{Key: "id", Value: recordID}}
	var rec Record
	if err := r.collection.FindOne(ctx, filter).Decode(&rec); err != nil {
		return domain.Record{}, fmt.Errorf("%w: %v", ErrFindingRecord, err)
	}

	return domain.Record{
		ID:        rec.ID,
		Timestamp: rec.Timestamp.Time(),
	}, nil
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
