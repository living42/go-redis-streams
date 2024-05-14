//+go build integration

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/badgerodon/go-redis-streams/consumer"
	"github.com/badgerodon/go-redis-streams/producer"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	client.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 1; i <= 2; i++ {
			p.XDel(ctx, fmt.Sprintf("test-stream-%d", i))
			p.XGroupDestroy(ctx, fmt.Sprintf("test-stream-%d", i), "test-group")
			p.XGroupCreateMkStream(ctx, fmt.Sprintf("test-stream-%d", i), "test-group", "$")
		}
		return nil
	})

	ids := make([]string, 2)
	for i := 1; i <= 2; i++ {
		p := producer.New(client, fmt.Sprintf("test-stream-%d", i))
		id, err := p.Write(ctx, producer.WithField("key", "value"))
		assert.NoError(t, err)
		ids[i-1] = id
	}

	// running the consumer twice, with no ack should reconsume the same messages
	for i := 0; i < 2; i++ {
		c := consumer.New(client, "test-group", "test-consumer",
			consumer.WithStream("test-stream-1"),
			consumer.WithStream("test-stream-2"))
		msgs, err := c.Read(ctx)
		assert.NoError(t, err)
		assert.Equal(t, []consumer.Message{
			{Stream: "test-stream-1", ID: ids[0], Values: map[string]interface{}{"key": "value"}},
			{Stream: "test-stream-2", ID: ids[1], Values: map[string]interface{}{"key": "value"}},
		}, msgs)
	}

	t.Run("with-ack", func(t *testing.T) {
		c := consumer.New(client, "test-group", "test-consumer",
			consumer.WithStream("test-stream-1"),
			consumer.WithStream("test-stream-2"),
			consumer.WithBlock(-1))
		msgs, err := c.Read(ctx)
		assert.NoError(t, err)
		assert.Equal(t, []consumer.Message{
			{Stream: "test-stream-1", ID: ids[0], Values: map[string]interface{}{"key": "value"}},
			{Stream: "test-stream-2", ID: ids[1], Values: map[string]interface{}{"key": "value"}},
		}, msgs)

		err = c.Ack(ctx, msgs...)
		assert.NoError(t, err)

		msgs, err = c.Read(ctx)
		assert.NoError(t, err)
		assert.Empty(t, msgs)
	})

}
