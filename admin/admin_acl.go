package admin

import (
	"context"
	"encoding/json"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type AclInfo struct {
	GlobalWhiteAddrs   []string            `json:"globalWhiteAddrs"`
	PlainAccessConfigs []PlainAccessConfig `json:"plainAccessConfigs"`
}

type PlainAccessConfig struct {
	Admin              bool     `json:"admin"`
	AccessKey          string   `json:"accessKey"`
	SecretKey          string   `json:"secretKey"`
	WhiteRemoteAddress string   `json:"whiteRemoteAddress"`
	TopicPerms         []string `json:"topicPerms"`
	GroupPerms         []string `json:"groupPerms"`
	DefaultTopicPerm   string   `json:"defaultTopicPerm"`
	DefaultGroupPerm   string   `json:"defaultGroupPerm"`
}

func (a *admin) GetBrokerClusterAclInfo(ctx context.Context, addr string) (*AclInfo, error) {
	aclConfig := AclInfo{}

	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterAclConfig, nil, nil)

	response, err := a.cli.InvokeSync(ctx, addr, cmd, 3*time.Second)

	if err != nil {
		rlog.Error("get broker cluster acl info error", map[string]interface{}{
			rlog.LogKeyBroker:        addr,
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("get broker cluster acl info success", map[string]interface{}{rlog.LogKeyBroker: addr})
	}

	repBody := utils.JavaFastJsonConvert(string(response.Body))
	return &aclConfig, json.Unmarshal([]byte(repBody), &aclConfig)
}
