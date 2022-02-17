package admin

// func (a *admin) GetBrokerClusterInfo(ctx context.Context, opts ...OptionCreate) error {
// cfg := defaultTopicConfigCreate()
// for _, apply := range opts {
// 	apply(&cfg)
// }

// request := &internal.CreateTopicRequestHeader{}

// cmd := remote.NewRemotingCommand(internal.ReqGetBrokerClusterInfo, request, nil)
// resp, err := a.cli.InvokeSync(ctx, a.namesrv.BrokerAddrList(), cmd, 5*time.Second)
// if err != nil {
// 	rlog.Error("create topic error", map[string]interface{}{
// 		rlog.LogKeyTopic:         cfg.Topic,
// 		rlog.LogKeyBroker:        cfg.BrokerAddr,
// 		rlog.LogKeyUnderlayError: err,
// 	})
// } else {
// 	fmt.Printf("resp.Body() = %v\n", string(resp.Body))
// 	rlog.Info("create topic success", map[string]interface{}{
// 		rlog.LogKeyTopic:  cfg.Topic,
// 		rlog.LogKeyBroker: cfg.BrokerAddr,
// 	})
// }
// return err
// 	return nil
// }
