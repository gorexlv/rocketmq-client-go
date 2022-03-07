/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"go.uber.org/multierr"
)

type Admin interface {
	CreateTopic(ctx context.Context, opts *createTopicOptions) error
	DeleteTopic(ctx context.Context, opts *deleteTopicOptions) error
	ListTopics(ctx context.Context, opts *listTopicsOptions) ([]string, error)
	CreateAndUpdateSubscriptionGroupConfig(ctx context.Context, opts *createSubscriptionGroupOptions) error

	GetBrokerClusterInfo(ctx context.Context, clusterName string) (map[string]internal.BrokerData, error)
	Close() error
}

type admin struct {
	cli     internal.RMQClient
	namesrv internal.Namesrvs

	opts *adminOptions

	closeOnce sync.Once
}

// NewAdmin initialize admin
func NewAdmin(opt *adminOptions) (Admin, error) {
	return newAdmin(opt)
}

func newAdmin(opts *adminOptions) (*admin, error) {
	namesrv, err := internal.NewNamesrv(opts.Resolver)
	if err != nil {
		return nil, err
	}
	opts.Namesrv = namesrv
	cli := internal.GetOrNewRocketMQClient(opts.ClientOptions, nil)
	//log.Printf("Client: %#v", namesrv.srvs)
	return &admin{
		cli:     cli,
		namesrv: namesrv,
		opts:    opts,
	}, nil
}

func (a *admin) ListTopics(ctx context.Context, opts *listTopicsOptions) ([]string, error) {
	if len(opts.NameSrvAddr) == 0 {
		opts.NameSrvAddr = a.namesrv.AddrList()
	}

	var ret = make([]string, 0)
	for _, nameSrvAddr := range opts.NameSrvAddr {
		command, err := a.fetchAllTopicListFromNameServer(ctx, nameSrvAddr)
		if err != nil {
			rlog.Error("fetch topics in nameserver error", map[string]interface{}{
				"nameServer":             nameSrvAddr,
				rlog.LogKeyUnderlayError: err,
			})
			return nil, err
		}
		switch command.Code {
		case internal.ResSuccess:
			fmt.Printf("topics: %v\n", string(command.Body))
		case internal.ResError:
			return nil, fmt.Errorf("list topics falid: %w", err)
		}
	}
	rlog.Info("fetch topics success", map[string]interface{}{
		"nameServer":      opts.NameSrvAddr,
		rlog.LogKeyBroker: opts.BrokerAddr,
	})

	return ret, nil
}

// Returns Broekr Cluster Info

func (a *admin) GetBrokerClusterInfo(ctx context.Context, clusterName string) (map[string]internal.BrokerData, error) {
	return a.namesrv.GetBrokerClusterInfo(clusterName)
}

// CreateTopic create topic.
// TODO: another implementation like sarama, without brokerAddr as input
func (a *admin) CreateTopic(ctx context.Context, opts *createTopicOptions) error {
	header := &internal.CreateTopicRequestHeader{
		Topic:           opts.Topic,
		DefaultTopic:    opts.DefaultTopic,
		ReadQueueNums:   opts.ReadQueueNums,
		WriteQueueNums:  opts.WriteQueueNums,
		Perm:            opts.Perm,
		TopicFilterType: opts.TopicFilterType,
		TopicSysFlag:    opts.TopicSysFlag,
		Order:           opts.Order,
	}

	if opts.BrokerAddr != "" {
		return a.createTopicInBroker(ctx, opts.BrokerAddr, header)
	}

	if opts.ClusterName != "" {
		return a.eachBroker(ctx, opts.ClusterName, func(addr string) error {
			return a.createTopicInBroker(ctx, addr, header)
		})
	}

	return nil
}

func (a *admin) CreateAndUpdateSubscriptionGroupConfig(ctx context.Context, opts *createSubscriptionGroupOptions) error {
	header := &internal.SubscriptionGroupConfigHeader{
		GroupName:                    opts.GroupName,
		ConsumeEnable:                opts.ConsumeEnable,
		ConsumeFromMinEnable:         opts.ConsumeFromMinEnable,
		ConsumeBroadcastEnable:       opts.ConsumeBroadcastEnable,
		RetryQueueNums:               opts.RetryQueueNums,
		RetryMaxTimes:                opts.RetryMaxTimes,
		BrokerID:                     0,
		WhichBrokerWhenConsumeSlowly: opts.WhichBrokerWhenConsumeSlowly,
	}

	if opts.BrokerAddr != "" {
		return a.createAndUpdateSubscriptionGroupConfig(ctx, opts.BrokerAddr, header)
	}

	if opts.ClusterName != "" {
		return a.eachBroker(ctx, opts.ClusterName, func(addr string) error {
			return a.createAndUpdateSubscriptionGroupConfig(ctx, addr, header)
		})
	}
	return nil
}

// DeleteTopic delete topic in both broker and nameserver.
func (a *admin) DeleteTopic(ctx context.Context, opts *deleteTopicOptions) error {
	//delete topic in broker
	if opts.BrokerAddr == "" {
		a.namesrv.UpdateTopicRouteInfo(opts.Topic)
		opts.BrokerAddr = a.namesrv.FindBrokerAddrByTopic(opts.Topic)
	}

	if _, err := a.deleteTopicInBroker(ctx, opts.Topic, opts.BrokerAddr); err != nil {
		rlog.Error("delete topic in broker error", map[string]interface{}{
			rlog.LogKeyTopic:         opts.Topic,
			rlog.LogKeyBroker:        opts.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
		return err
	}

	//delete topic in nameserver
	if len(opts.NameSrvAddr) == 0 {
		_, _, err := a.namesrv.UpdateTopicRouteInfo(opts.Topic)
		if err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				rlog.LogKeyTopic:         opts.Topic,
				rlog.LogKeyUnderlayError: err,
			})
		}
		opts.NameSrvAddr = a.namesrv.AddrList()
	}

	for _, nameSrvAddr := range opts.NameSrvAddr {
		if _, err := a.deleteTopicInNameServer(ctx, opts.Topic, nameSrvAddr); err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				"nameServer":             nameSrvAddr,
				rlog.LogKeyTopic:         opts.Topic,
				rlog.LogKeyUnderlayError: err,
			})
			return err
		}
	}
	rlog.Info("delete topic success", map[string]interface{}{
		"nameServer":      opts.NameSrvAddr,
		rlog.LogKeyTopic:  opts.Topic,
		rlog.LogKeyBroker: opts.BrokerAddr,
	})
	return nil
}

func (a *admin) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}

func (a *admin) createAndUpdateSubscriptionGroupConfig(ctx context.Context, addr string, header *internal.SubscriptionGroupConfigHeader) (err error) {
	cmd := remote.NewRemotingCommand(internal.ReqUpdateAndCreateSubscriptionGroup, nil, header.ToJSON())
	cmd, err = a.cli.InvokeSync(ctx, addr, cmd, 5*time.Second)
	if err != nil {
		rlog.Error("create subscription group error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return
	} else {
		rlog.Info("create subscription group success", map[string]interface{}{})
	}
	switch cmd.Code {
	case internal.ResSuccess:
		return nil
	case internal.ResError:
		return fmt.Errorf("createAndUpdateSubscriptionGroupConfig failed: %s", cmd.Remark)
	}
	return nil
}

func (a *admin) createTopicInBroker(ctx context.Context, brokerAddr string, header *internal.CreateTopicRequestHeader) error {
	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, header, nil)
	_, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
	if err != nil {
		rlog.Error("create topic error", map[string]interface{}{
			rlog.LogKeyTopic:         header.Topic,
			rlog.LogKeyBroker:        brokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
	} else {
		rlog.Info("create topic success", map[string]interface{}{
			rlog.LogKeyTopic:  header.Topic,
			rlog.LogKeyBroker: brokerAddr,
		})
	}
	return err
}

func (a *admin) fetchAllTopicListFromNameServer(ctx context.Context, nameSrvAddr string) (*remote.RemotingCommand, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllTopicListFromNameServer, nil, nil)
	return a.cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

func (a *admin) getBrokerClusterInfo(ctx context.Context, cluster string, nameSrvAddr string) (*remote.RemotingCommand, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
	return a.cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

// DeleteTopicInBroker delete topic in broker.
func (a *admin) deleteTopicInBroker(ctx context.Context, topic string, brokerAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInBroker, request, nil)
	return a.cli.InvokeSync(ctx, brokerAddr, cmd, 5*time.Second)
}

// DeleteTopicInNameServer delete topic in nameserver.
func (a *admin) deleteTopicInNameServer(ctx context.Context, topic string, nameSrvAddr string) (*remote.RemotingCommand, error) {
	request := &internal.DeleteTopicRequestHeader{
		Topic: topic,
	}

	cmd := remote.NewRemotingCommand(internal.ReqDeleteTopicInNameSrv, request, nil)
	return a.cli.InvokeSync(ctx, nameSrvAddr, cmd, 5*time.Second)
}

func (a *admin) eachBroker(ctx context.Context, clusterName string, exec func(string) error) error {
	var merr error
	addresses := a.namesrv.BrokerAddrList(clusterName)
	if len(addresses) == 0 {
		rlog.Error("empth broker address list", map[string]interface{}{
			rlog.LogKeyCluster: clusterName,
		})
		return errors.New("empty broker address list")
	}

	for _, addr := range addresses {
		merr = multierr.Append(merr, exec(addr))
	}
	return merr
}
