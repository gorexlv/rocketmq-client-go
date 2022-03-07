package admin

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/assert"
)

var nameSrvAddr = []string{"127.0.0.1:9876"}

func TestAdmin_CreateTopic(t *testing.T) {
	topic := "newOne"
	clusterName := "DefaultCluster"
	testAdmin, err := newAdmin(AdminOptions().WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	if err != nil {
		t.Errorf("new admin: %v", err)
	}
	assert.Nil(t, err)
	defer testAdmin.Close()

	options := CreateTopicOptions()
	options.Topic = topic
	options.ClusterName = clusterName
	if err := testAdmin.CreateTopic(context.Background(), options); err != nil {
		t.Errorf("new admin: %v\n", err)
	}
}

func TestMain_ListTopics(t *testing.T) {
	clusterName := "DefaultCluster"
	testAdmin, err := newAdmin(AdminOptions().WithPassthroughResolver(nameSrvAddr))
	if err != nil {
		t.Errorf("new admin: %v", err)
	}
	assert.Nil(t, err)
	defer testAdmin.Close()

	options := ListTopicOptions()
	options.ClusterName = clusterName
	if topics, err := testAdmin.ListTopics(context.Background(), options); err != nil {
		t.Errorf("new admin: %v\n", err)
	} else {
		t.Logf("new admin: %v\n", topics)
	}
}

func TestAdmin_getBrokerClusterInfo(t *testing.T) {
	clusterName := "DefaultCluster"
	testAdmin, err := newAdmin(AdminOptions().WithPassthroughResolver(nameSrvAddr))
	if err != nil {
		t.Errorf("new admin: %v", err)
	}
	assert.Nil(t, err)
	defer testAdmin.Close()

	command, err := testAdmin.getBrokerClusterInfo(context.Background(), clusterName, nameSrvAddr[0])
	assert.Nil(t, err)
	fmt.Printf("command = %v\n", string(command.Body))
}

func TestAdmin_fetchAllTopicListFromNameServer(t *testing.T) {
	testAdmin, err := newAdmin(AdminOptions().WithPassthroughResolver(nameSrvAddr))
	if err != nil {
		t.Errorf("new admin: %v", err)
	}
	assert.Nil(t, err)
	defer testAdmin.Close()

	command, err := testAdmin.fetchAllTopicListFromNameServer(context.Background(), nameSrvAddr[0])
	assert.Nil(t, err)
	fmt.Printf("command = %v\n", string(command.Body))
}

func Test_admin_createAndUpdateSubscriptionGroupConfig(t *testing.T) {
	testAdmin, err := newAdmin(AdminOptions().WithPassthroughResolver(nameSrvAddr))
	if err != nil {
		t.Fatalf("new admin: %v", err)
	}
	assert.Nil(t, err)
	defer testAdmin.Close()

	err = testAdmin.CreateAndUpdateSubscriptionGroupConfig(context.Background(), &createSubscriptionGroupOptions{
		GroupName:                    "foobar",
		ConsumeEnable:                true,
		ConsumeFromMinEnable:         true,
		ConsumeBroadcastEnable:       true,
		RetryQueueNums:               1,
		RetryMaxTimes:                16,
		WhichBrokerWhenConsumeSlowly: 1,
		ClusterName:                  "DefaultCluster",
	})
	assert.Nil(t, err)
}
