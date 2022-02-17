package internal

import (
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
)

type BrokerClusterData struct {
	BrokerAddrTable  map[string]BrokerData `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string   `json:"clusterAddrTable"`
}

func (brokerData *BrokerClusterData) decode(data string) error {
	res := gjson.Parse(data)
	err := jsoniter.Unmarshal([]byte(res.Get("clusterAddrTable").String()), &brokerData.ClusterAddrTable)

	if err != nil {
		return err
	}

	bds := res.Get("brokerAddrTable").Map()
	brokerData.BrokerAddrTable = make(map[string]BrokerData)
	for name, v := range bds {
		bd := BrokerData{
			BrokerName:      v.Get("brokerName").String(),
			Cluster:         v.Get("cluster").String(),
			BrokerAddresses: make(map[int64]string),
		}
		addrs := v.Get("brokerAddrs").String()
		strs := strings.Split(addrs[1:len(addrs)-1], ",")
		for _, str := range strs {
			i := strings.Index(str, ":")
			if i < 0 {
				continue
			}
			id, _ := strconv.ParseInt(str[0:i], 10, 64)
			bd.BrokerAddresses[id] = strings.Replace(str[i+1:], "\"", "", -1)
		}
		brokerData.BrokerAddrTable[name] = bd
	}
	return nil
}
