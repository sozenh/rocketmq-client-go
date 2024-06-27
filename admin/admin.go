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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type Admin interface {
	CreateTopic(ctx context.Context, opts ...OptionCreate) error
	DeleteTopic(ctx context.Context, opts ...OptionDelete) error

	GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error)
	FetchAllTopicList(ctx context.Context) (*TopicList, error)
	//GetBrokerClusterInfo(ctx context.Context) (*remote.RemotingCommand, error)
	FetchClusterInfo(ctx context.Context) (*ClusterInfo, error)
	FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error)
	FetchAllTopicConfig(ctx context.Context, brokerAddr string) (*AllTopicConfig, error)
	UpdateAclConfig(ctx context.Context, aclFunc ...AclFuncOption) error
	DeleteAclConfig(ctx context.Context, accessKey string) error
	PutOrderKVConfig(ctx context.Context, key, value string) error
	GetOrderKVConfig(ctx context.Context, key string) (val string, err error)
	DeleteOrderKVConfig(ctx context.Context, key string) (err error)
	GetKVListByNamespace(ctx context.Context, namespace string) (kvList *AllKVList, err error)

	// for broker
	UpdateBrokerConfig(ctx context.Context, key, value string) error
	FetchBrokerRuntimeInfo(ctx context.Context, addr string) (map[string]string, error)

	// for consumer

	Close() error
}

// TODO: move outdated context to ctx
type adminOptions struct {
	internal.ClientOptions
}

type AdminOption func(options *adminOptions)

func defaultAdminOptions() *adminOptions {
	opts := &adminOptions{
		ClientOptions: internal.DefaultClientOptions(),
	}
	opts.GroupName = "TOOLS_ADMIN"
	opts.InstanceName = time.Now().String()
	return opts
}

// WithResolver nameserver resolver to fetch nameserver addr
func WithResolver(resolver primitive.NsResolver) AdminOption {
	return func(options *adminOptions) {
		options.Resolver = resolver
	}
}

func WithCredentials(c primitive.Credentials) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Credentials = c
	}
}

// WithNamespace set the namespace of admin
func WithNamespace(namespace string) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.Namespace = namespace
	}
}

func WithTls(useTls bool) AdminOption {
	return func(options *adminOptions) {
		options.ClientOptions.RemotingClientConfig.UseTls = useTls
	}
}

type admin struct {
	cli internal.RMQClient

	opts *adminOptions

	closeOnce sync.Once
}

// NewAdmin initialize admin
func NewAdmin(opts ...AdminOption) (*admin, error) {
	defaultOpts := defaultAdminOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}
	namesrv, err := internal.NewNamesrv(defaultOpts.Resolver, defaultOpts.RemotingClientConfig)
	defaultOpts.Namesrv = namesrv
	if err != nil {
		return nil, err
	}

	cli := internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil)
	if cli == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = cli.GetNameSrv()
	//log.Printf("Client: %#v", namesrv.srvs)
	return &admin{
		cli:  cli,
		opts: defaultOpts,
	}, nil
}

func (a *admin) GetAllSubscriptionGroup(ctx context.Context, brokerAddr string, timeoutMillis time.Duration) (*SubscriptionGroupWrapper, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllSubscriptionGroupConfig, nil, nil)
	a.cli.RegisterACL()
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, timeoutMillis)
	if err != nil {
		rlog.Error("Get all group list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Get all group list success", map[string]interface{}{})
	}
	var subscriptionGroupWrapper SubscriptionGroupWrapper
	_, err = subscriptionGroupWrapper.Decode(response.Body, &subscriptionGroupWrapper)
	if err != nil {
		rlog.Error("Get all group list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &subscriptionGroupWrapper, nil
}

func (a *admin) FetchAllTopicList(ctx context.Context) (*TopicList, error) {
	cmd := remote.NewRemotingCommand(internal.ReqGetAllTopicListFromNameServer, nil, nil)
	response, err := a.cli.InvokeSync(ctx, a.cli.GetNameSrv().AddrList()[0], cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all topic list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("Fetch all topic list success", map[string]interface{}{})
	}
	var topicList TopicList
	_, err = topicList.Decode(response.Body, &topicList)
	if err != nil {
		rlog.Error("Fetch all topic list decode error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	}
	return &topicList, nil
}

func (a *admin) FetchClusterInfo(ctx context.Context) (clusterInfo *ClusterInfo, err error) {
	clusterInfo = new(ClusterInfo)
	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, nil, nil)
	response, err := a.cli.InvokeSync(ctx, a.cli.GetNameSrv().AddrList()[0], cmd, 3*time.Second)
	if err != nil {
		rlog.Error("Fetch all cluster list error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return
	}
	rlog.Info("Fetch all cluster list success", map[string]interface{}{})
	repBody := utils.JavaFastJsonConvert(string(response.Body))
	err = json.Unmarshal([]byte(repBody), clusterInfo)
	return
}

// CreateTopic create topic.
// TODO: another implementation like sarama, without brokerAddr as input
func (a *admin) CreateTopic(ctx context.Context, opts ...OptionCreate) error {
	cfg := defaultTopicConfigCreate()
	for _, apply := range opts {
		apply(&cfg)
	}

	request := &internal.CreateTopicRequestHeader{
		Topic:           cfg.Topic,
		DefaultTopic:    cfg.DefaultTopic,
		ReadQueueNums:   cfg.ReadQueueNums,
		WriteQueueNums:  cfg.WriteQueueNums,
		Perm:            cfg.Perm,
		TopicFilterType: cfg.TopicFilterType,
		TopicSysFlag:    cfg.TopicSysFlag,
		Order:           cfg.Order,
	}
	a.cli.RegisterACL()
	cmd := remote.NewRemotingCommand(internal.ReqCreateTopic, request, nil)
	_, err := a.cli.InvokeSync(ctx, cfg.BrokerAddr, cmd, 5*time.Second)
	if err != nil {
		rlog.Error("create topic error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
	} else {
		rlog.Info("create topic success", map[string]interface{}{
			rlog.LogKeyTopic:  cfg.Topic,
			rlog.LogKeyBroker: cfg.BrokerAddr,
		})
	}
	return err
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

// DeleteTopic delete topic in both broker and nameserver.
func (a *admin) DeleteTopic(ctx context.Context, opts ...OptionDelete) error {
	cfg := defaultTopicConfigDelete()
	for _, apply := range opts {
		apply(&cfg)
	}
	//delete topic in broker
	if cfg.BrokerAddr == "" {
		a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.BrokerAddr = a.cli.GetNameSrv().FindBrokerAddrByTopic(cfg.Topic)
	}

	if _, err := a.deleteTopicInBroker(ctx, cfg.Topic, cfg.BrokerAddr); err != nil {
		rlog.Error("delete topic in broker error", map[string]interface{}{
			rlog.LogKeyTopic:         cfg.Topic,
			rlog.LogKeyBroker:        cfg.BrokerAddr,
			rlog.LogKeyUnderlayError: err,
		})
		return err
	}

	//delete topic in nameserver
	if len(cfg.NameSrvAddr) == 0 {
		a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		cfg.NameSrvAddr = a.cli.GetNameSrv().AddrList()
		_, _, err := a.cli.GetNameSrv().UpdateTopicRouteInfo(cfg.Topic)
		if err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
		}
		cfg.NameSrvAddr = a.cli.GetNameSrv().AddrList()
	}

	for _, nameSrvAddr := range cfg.NameSrvAddr {
		if _, err := a.deleteTopicInNameServer(ctx, cfg.Topic, nameSrvAddr); err != nil {
			rlog.Error("delete topic in nameserver error", map[string]interface{}{
				"nameServer":             nameSrvAddr,
				rlog.LogKeyTopic:         cfg.Topic,
				rlog.LogKeyUnderlayError: err,
			})
			return err
		}
	}
	rlog.Info("delete topic success", map[string]interface{}{
		"nameServer":      cfg.NameSrvAddr,
		rlog.LogKeyTopic:  cfg.Topic,
		rlog.LogKeyBroker: cfg.BrokerAddr,
	})
	return nil
}

func (a *admin) FetchPublishMessageQueues(ctx context.Context, topic string) ([]*primitive.MessageQueue, error) {
	return a.cli.GetNameSrv().FetchPublishMessageQueues(utils.WrapNamespace(a.opts.Namespace, topic))
}

func (a *admin) Close() error {
	a.closeOnce.Do(func() {
		a.cli.Shutdown()
	})
	return nil
}

func (a *admin) FetchAllTopicConfig(ctx context.Context, brokerAddr string) (allTopicConfigList *AllTopicConfig, err error) {
	allTopicConfigList = new(AllTopicConfig)
	a.cli.RegisterACL()
	cmd := remote.NewRemotingCommand(internal.ReqGetAllTopicConfig, nil, nil)
	response, err := a.cli.InvokeSync(ctx, brokerAddr, cmd, 20*time.Second)
	if err != nil {
		rlog.Error("fetch all topic config error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return
	}
	if response.Code != internal.ResSuccess {
		rlog.Error("fetch all topic config error", map[string]interface{}{
			"response": response,
		})
		err = fmt.Errorf("fetch all topic config response : %v", response)
		return
	}

	rlog.Info("fetch all topic config success", map[string]interface{}{})
	err = json.Unmarshal(response.Body, allTopicConfigList)
	return
}

func (a *admin) UpdateBrokerConfig(ctx context.Context, key, value string) (err error) {
	var cfBody = []byte(fmt.Sprintf("%s=%s", key, value))
	var clusterInfo *ClusterInfo
	if clusterInfo, err = a.FetchClusterInfo(ctx); err != nil {
		rlog.Error("get cluster info error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
	a.cli.RegisterACL()
	for brokerName, brokeTable := range clusterInfo.BrokerAddrTable {
		brokerAddrList := make([]string, 0)
		for _, brokerAddr := range brokeTable.BrokerAddrs {
			brokerAddrList = append(brokerAddrList, brokerAddr)
		}
		if err = a.updateBrokerConfig(ctx, internal.UpdateBrokerConfig, nil, cfBody, brokerAddrList, brokerName); err != nil {
			return
		}
	}
	return
}

func (a *admin) updateBrokerConfig(ctx context.Context, reqCode int16,
	header remote.CustomHeader, cfBody []byte,
	brokerAddrList []string,
	brokerName string,
) (err error) {
	for _, brokerAddr := range brokerAddrList {
		var response *remote.RemotingCommand
		cmd := remote.NewRemotingCommand(reqCode, header, cfBody)
		response, err = a.cli.InvokeSync(ctx, brokerAddr, cmd, 20*time.Second)
		if err != nil {
			rlog.Error("update broker config error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
				"brokerName":             brokerName,
				"brokerAddr":             brokerAddr,
			})
			return
		}
		if response.Code != internal.ResSuccess {
			rlog.Error("update broker config error", map[string]interface{}{
				"brokerName": brokerName,
				"brokerAddr": brokerAddr,
				"response":   response,
			})
			err = fmt.Errorf("update broker config error, response %v", response)
			return
		}
		rlog.Info("update config success", map[string]interface{}{
			"brokerName": brokerName,
			"brokerAddr": brokerAddr,
		})
	}
	return
}

func (a *admin) UpdateAclConfig(ctx context.Context, aclFunc ...AclFuncOption) (err error) {
	var clusterInfo *ClusterInfo
	aclConf := NewAclConfig(aclFunc...)
	aclConfig := &internal.UpdateAclConfigRequestHeader{
		AccessKey:          aclConf.AccessKey,
		SecretKey:          aclConf.SecretKey,
		WhiteRemoteAddress: aclConf.WhiteRemoteAddress,
		DefaultTopicPerm:   aclConf.DefaultTopicPerm,
		DefaultGroupPerm:   aclConf.DefaultGroupPerm,
		Admin:              aclConf.Admin,
		TopicPerms:         aclConf.TopicPerms,
		GroupPerms:         aclConf.GroupPerms,
	}
	if clusterInfo, err = a.FetchClusterInfo(ctx); err != nil {
		rlog.Error("get cluster info error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
	a.cli.RegisterACL()
	for brokerName, brokeTable := range clusterInfo.BrokerAddrTable {
		brokerAddrList := make([]string, 0)
		for _, brokerAddr := range brokeTable.BrokerAddrs {
			brokerAddrList = append(brokerAddrList, brokerAddr)
		}
		if err = a.updateBrokerConfig(ctx, internal.UpdateAndCreateAclConfig, aclConfig, nil, brokerAddrList, brokerName); err != nil {
			return
		}
	}
	return
}

func (a *admin) DeleteAclConfig(ctx context.Context, accessKey string) (err error) {
	var clusterInfo *ClusterInfo
	aclConfig := &internal.DeleteAclConfigRequestHeader{
		AccessKey: accessKey,
	}
	if clusterInfo, err = a.FetchClusterInfo(ctx); err != nil {
		rlog.Error("get cluster info error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
	a.cli.RegisterACL()
	for brokerName, brokeTable := range clusterInfo.BrokerAddrTable {
		brokerAddrList := make([]string, 0)
		for _, brokerAddr := range brokeTable.BrokerAddrs {
			brokerAddrList = append(brokerAddrList, brokerAddr)
		}
		if err = a.updateBrokerConfig(ctx, internal.DeleteAclConfig, aclConfig, nil, brokerAddrList, brokerName); err != nil {
			return
		}
	}
	return
}

// isCluster false
// key: topicName
// value: "rocketmq-2a597b8d-0:WriteQueueNums;rocketmq-2a597b8d-1:WriteQueueNums"
func (a *admin) PutOrderKVConfig(ctx context.Context, key, value string) (err error) {
	return a.putKVConfig(ctx, key, value, OrderTopicConfig)
}

func (a *admin) GetOrderKVConfig(ctx context.Context, key string) (val string, err error) {
	return a.getKVConfig(ctx, key, OrderTopicConfig)
}

func (a *admin) DeleteOrderKVConfig(ctx context.Context, key string) (err error) {
	return a.deleteKVConfig(ctx, key, OrderTopicConfig)
}

func (a *admin) GetKVListByNamespace(ctx context.Context, namespace string) (kvList *AllKVList, err error) {
	var (
		response        *remote.RemotingCommand
		nameServerAddrs = a.opts.Resolver.Resolve()
	)
	kvList = new(AllKVList)
	a.cli.RegisterACL()
	head := &internal.KVConfigRequestHeader{
		Namespace: namespace,
	}

	cmd := remote.NewRemotingCommand(internal.GetKVListByNamespace, head, nil)
	response, err = a.cli.InvokeSync(ctx, nameServerAddrs[0], cmd, 10*time.Second)
	if err != nil {
		rlog.Error("get kv config error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return
	}

	// not found
	if response.Code == internal.ResQueryNotFound {
		return
	}

	if response.Code != internal.ResSuccess {
		rlog.Error("get kv config error", map[string]interface{}{
			"response": response,
		})
		err = fmt.Errorf("get kv config error response : %v", response)
		return
	}
	// {"table":{"Gong123123":"rocketmq-4156c457-0:1","TEst1·23123123":"rocketmq-4156c457-0:1","Gongxulei02":"rocketmq-4156c457-0:1"}}
	err = json.Unmarshal(response.Body, kvList)
	return
}

func (a *admin) putKVConfig(ctx context.Context, key, value, namespace string) (err error) {
	var (
		response        *remote.RemotingCommand
		nameServerAddrs = a.opts.Resolver.Resolve()
	)
	a.cli.RegisterACL()
	head := &internal.KVConfigRequestHeader{
		Namespace: namespace,
		Key:       key,
		Value:     value,
	}
	for _, nsAddr := range nameServerAddrs {
		cmd := remote.NewRemotingCommand(internal.PutKVConfig, head, nil)
		response, err = a.cli.InvokeSync(ctx, nsAddr, cmd, 10*time.Second)
		if err != nil {
			rlog.Error("set kv config error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			return
		}
		if response.Code != internal.ResSuccess {
			rlog.Error("set kv config error", map[string]interface{}{
				"response": response,
			})
			err = fmt.Errorf("set kv config error response : %v", response)
			return
		}
	}
	return
}

func (a *admin) getKVConfig(ctx context.Context, key, namespace string) (val string, err error) {
	var (
		response        *remote.RemotingCommand
		nameServerAddrs = a.opts.Resolver.Resolve()
	)
	a.cli.RegisterACL()
	head := &internal.KVConfigRequestHeader{
		Namespace: namespace,
		Key:       key,
	}

	cmd := remote.NewRemotingCommand(internal.GetKVConfig, head, nil)
	response, err = a.cli.InvokeSync(ctx, nameServerAddrs[0], cmd, 10*time.Second)
	if err != nil {
		rlog.Error("get kv config error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return
	}

	// not found
	if response.Code == internal.ResQueryNotFound {
		return
	}
	if response.Code != internal.ResSuccess {
		rlog.Error("get kv config error", map[string]interface{}{
			"response": response,
		})
		err = fmt.Errorf("get kv config error response : %v", response)
		return
	}
	val = response.ExtFields["value"]
	return
}

func (a *admin) deleteKVConfig(ctx context.Context, key, namespace string) (err error) {
	var (
		response        *remote.RemotingCommand
		nameServerAddrs = a.opts.Resolver.Resolve()
	)
	a.cli.RegisterACL()
	head := &internal.KVConfigRequestHeader{
		Namespace: namespace,
		Key:       key,
	}
	for _, nsAddr := range nameServerAddrs {
		cmd := remote.NewRemotingCommand(internal.DeleteKVConfig, head, nil)
		response, err = a.cli.InvokeSync(ctx, nsAddr, cmd, 10*time.Second)
		if err != nil {
			rlog.Error("delete kv config error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			return
		}
		if response.Code != internal.ResSuccess {
			rlog.Error("delete kv config error", map[string]interface{}{
				"response": response,
			})
			err = fmt.Errorf("delete kv config error response : %v", response)
			return
		}
	}
	return
}
