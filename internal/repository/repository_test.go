package repository_test

import (
	"context"
	"mongo-ttl/internal/domain"
	"mongo-ttl/internal/repository"
	"testing"
	"time"

	uuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestRepository_StoreRecord(t *testing.T) {
	t.Run("should store a record", func(t *testing.T) {
		ctx := context.Background()
		collection := setupDB(t, ctx)
		someRecord := domain.Record{
			ID:        uuid.New(),
			Timestamp: time.Now(),
		}

		repo, err := repository.NewRepository(ctx, collection, 700)
		require.NoError(t, err)

		err = repo.StoreRecord(ctx, someRecord)
		require.NoError(t, err)

		record, err := repo.GetRecord(ctx, someRecord.ID)
		require.NoError(t, err)

		assert.Equal(t, someRecord.ID, record.ID)
		assert.Equal(t, someRecord.Timestamp.UTC().Truncate(time.Millisecond), record.Timestamp)
	})

	t.Run("should return a duplicate error if we try to insert the same record", func(t *testing.T) {
		ctx := context.Background()
		collection := setupDB(t, ctx)
		someRecord := domain.Record{
			ID:        uuid.New(),
			Timestamp: time.Now(),
		}

		repo, err := repository.NewRepository(ctx, collection, 700)
		require.NoError(t, err)

		err = repo.StoreRecord(ctx, someRecord)
		require.NoError(t, err)

		err = repo.StoreRecord(ctx, someRecord)
		assert.ErrorIs(t, err, repository.ErrDuplicateRecord)
	})
}

func TestRepository_GetRecord(t *testing.T) {
	t.Run("should get a record", func(t *testing.T) {})

	t.Run("should return a error finding record if it doesn't exist", func(t *testing.T) {})
}

func setupDB(t *testing.T, ctx context.Context) *mongo.Collection {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://root:example@localhost:27017"))
	if err != nil {
		require.NoError(t, err)
	}
	return client.Database("ttl").Collection("records")
}
