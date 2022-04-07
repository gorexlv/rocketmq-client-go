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
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type adminOptions struct {
	internal.ClientOptions
}

// AdminOptions represents adminOptions with default values
func AdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_ADMIN"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func (options *adminOptions) WithResolver(resolver primitive.NsResolver) *adminOptions {
	options.Resolver = resolver
	return options
}

func (options *adminOptions) WithPassthroughResolver(addrs []string) *adminOptions {
	options.Resolver = primitive.NewPassthroughResolver(addrs)
	return options
}

type createTopicOptions struct {
	Topic           string
	BrokerAddr      string
	DefaultTopic    string
	ReadQueueNums   int
	WriteQueueNums  int
	Perm            int
	TopicFilterType string
	TopicSysFlag    int
	Order           bool
	ClusterName     string
}

func (options *createTopicOptions) WithTopic(topic string) *createTopicOptions {
	options.Topic = topic
	return options
}

func (options *createTopicOptions) WithBrokerAddr(brokerAddr string) *createTopicOptions {
	options.BrokerAddr = brokerAddr
	return options
}

func (options *createTopicOptions) WithClusterName(clusterName string) *createTopicOptions {
	options.ClusterName = clusterName
	return options
}

func (options *createTopicOptions) WithOrder(order bool) *createTopicOptions {
	options.Order = order
	return options
}

func (options *createTopicOptions) WithReadQueueNum(num int) *createTopicOptions {
	options.ReadQueueNums = num
	return options
}

func (options *createTopicOptions) WithWriteQueueNum(num int) *createTopicOptions {
	options.WriteQueueNums = num
	return options
}

// CreateTopicOptions represents createTopicOptions with default setting.
func CreateTopicOptions() *createTopicOptions {
	opts := &createTopicOptions{
		DefaultTopic:    "defaultTopic",
		ReadQueueNums:   8,
		WriteQueueNums:  8,
		Perm:            6,
		TopicFilterType: "SINGLE_TAG",
		TopicSysFlag:    0,
		Order:           false,
	}
	return opts
}

type deleteTopicOptions struct {
	Topic       string
	ClusterName string
	NameSrvAddr []string
	BrokerAddr  string
}

func (options *deleteTopicOptions) WithTopic(topic string) *deleteTopicOptions {
	options.Topic = topic
	return options
}

func (options *deleteTopicOptions) WithClusterName(clusterName string) *deleteTopicOptions {
	options.ClusterName = clusterName
	return options
}

func (options *deleteTopicOptions) WithBrokerAddr(brokerAddr string) *deleteTopicOptions {
	options.BrokerAddr = brokerAddr
	return options
}
func (options *deleteTopicOptions) WithNameServerAddrs(addrs []string) *deleteTopicOptions {
	options.NameSrvAddr = addrs
	return options
}

// DeleteTopicOptions represents deleteTopicOptions with default settings.
func DeleteTopicOptions() *deleteTopicOptions {
	opts := &deleteTopicOptions{}
	return opts
}

type listTopicsOptions struct {
	NameSrvAddr []string
	BrokerAddr  string
	ClusterName string
}

func (opts *listTopicsOptions) WithBrokerAddr(addr string) *listTopicsOptions {
	opts.BrokerAddr = addr
	return opts
}

func (opts *listTopicsOptions) WithClusterName(clusterName string) *listTopicsOptions {
	opts.ClusterName = clusterName
	return opts
}

func (opts *listTopicsOptions) WithNameServerAddrs(addrs []string) *listTopicsOptions {
	opts.NameSrvAddr = addrs
	return opts
}

// ListTopicOptions represents listTopicOptions with default values.
func ListTopicOptions() *listTopicsOptions {
	return &listTopicsOptions{}
}

type createSubscriptionGroupOptions struct {
	GroupName                    string
	ConsumeEnable                bool
	ConsumeBroadcastEnable       bool
	ConsumeFromMinEnable         bool
	RetryQueueNums               int
	RetryMaxTimes                int
	WhichBrokerWhenConsumeSlowly int64
	BrokerAddr                   string
	ClusterName                  string
}

// CreateSubscriptionGroupOptions represents createSubscriptionGroupOptions with default settings.
func CreateSubscriptionGroupOptions() *createSubscriptionGroupOptions {
	return &createSubscriptionGroupOptions{
		ConsumeEnable:                true,
		ConsumeFromMinEnable:         true,
		ConsumeBroadcastEnable:       true,
		RetryQueueNums:               1,
		RetryMaxTimes:                16,
		WhichBrokerWhenConsumeSlowly: 1,
	}
}

func (opts *createSubscriptionGroupOptions) WithClusterName(clusterName string) *createSubscriptionGroupOptions {
	opts.ClusterName = clusterName
	return opts
}

func (opts *createSubscriptionGroupOptions) WithGroupName(groupName string) *createSubscriptionGroupOptions {
	opts.GroupName = groupName
	return opts
}
