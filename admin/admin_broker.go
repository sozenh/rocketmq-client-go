package admin

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/sozenh/rocketmq-client-go/v2/internal"
	"github.com/sozenh/rocketmq-client-go/v2/internal/remote"
	"github.com/sozenh/rocketmq-client-go/v2/internal/utils"
	"github.com/sozenh/rocketmq-client-go/v2/rlog"
)

func (a *admin) GetBrokerConfig(ctx context.Context, addr string) (map[string]string, error) {
	brokerConfig := map[string]string{}

	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerConfig, nil, nil)

	response, err := a.cli.InvokeSync(ctx, addr, cmd, 3*time.Second)

	if err != nil {
		rlog.Error("get broker config error", map[string]interface{}{
			rlog.LogKeyBroker:        addr,
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("get broker config success", map[string]interface{}{rlog.LogKeyBroker: addr})
	}

	repBody := utils.JavaFastJsonConvert(string(response.Body))

	lines := strings.Split(repBody, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(strings.TrimSpace(parts[0]))
			value := strings.TrimSpace(strings.TrimSpace(parts[1]))

			brokerConfig[key] = value
		}
	}

	return brokerConfig, nil
}

func (a *admin) FetchBrokerRuntimeInfo(ctx context.Context, addr string) (map[string]string, error) {
	var brokerInfo AllKVList

	cmd := remote.NewRemotingCommand(internal.ReqGetBrokerRuntimeInfo, nil, nil)
	response, err := a.cli.InvokeSync(ctx, addr, cmd, 3*time.Second)
	if err != nil {
		rlog.Error("get broker runtime info error", map[string]interface{}{
			rlog.LogKeyBroker:        addr,
			rlog.LogKeyUnderlayError: err,
		})
		return nil, err
	} else {
		rlog.Info("get broker runtime info success", map[string]interface{}{rlog.LogKeyBroker: addr})
	}
	repBody := utils.JavaFastJsonConvert(string(response.Body))

	return brokerInfo.Table, json.Unmarshal([]byte(repBody), &brokerInfo)
}
