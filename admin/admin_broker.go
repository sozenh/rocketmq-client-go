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
	err = json.Unmarshal([]byte(repBody), &brokerInfo)
	return brokerInfo.Table, nil
}
