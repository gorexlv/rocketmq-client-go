package admin_test

import (
	"context"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/assert"
)

func TestAdmin(t *testing.T) {
	// nameSrvAddr := []string{"127.0.0.1:9876"}
	nameSrvAddr := []string{"10.1.41.171:9876"}
	// topic := "newOne"
	// brokerAddr := "127.0.0.1:10911"
	// brokerAddr := "10.238.63.24:10911"

	testAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	if err != nil {
		t.Errorf("new admin: %v", err)
	}
	assert.Nil(t, err)
	defer testAdmin.Close()

	testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate("newOne4"),
		admin.WithCluster2Name("DefaultCluster"),
		// admin.WithBrokerAddrCreate(brokerAddr),
	)

	// testAdmin.ListTopics()
}
