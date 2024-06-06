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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	//clusterName := "DefaultCluster"
	// nameSrvAddr := []string{"10.10.88.243:27852"}
	nameSrvAddr := []string{"10.10.88.152:9876"}
	// rocketmq-7b98698f-0 0:10.10.88.243:25035
	brokerAddr := "10.10.88.243:25035"
	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			// AccessKey: "rocketAdmin",
			AccessKey: "rocketmq2",
			// SecretKey: "BHZ1P97DUI4JEO1XU2ZLCE9OG6JV",
			SecretKey: "12345678",
		}),
	)
	// res, _ := testAdmin.FetchClusterInfo(context.Background())
	// fmt.Println(res)

	// ures, err := testAdmin.UpdateBrokerConfig(context.Background(), "fileReservedTime", "16")
	// fmt.Println(ures)
	// fmt.Println(err)

	aclRes, err := testAdmin.UpdateAclConfig(context.Background(), &internal.UpdateAclConfigRequestHeader{
		AccessKey:          "gxltest0001",
		SecretKey:          "TtestPSHHSHHAJKSHKJAHSK",
		WhiteRemoteAddress: "",
		DefaultTopicPerm:   "PUB",
		DefaultGroupPerm:   "PUB",
		Admin:              "false",
		TopicPerms:         "ddddxxxx=DENY,topictest=PUB",
		GroupPerms:         "ggggxxxx=DENY,grouptest=PUB",
	})
	fmt.Println(aclRes, err)

	return
	// group list
	result, err := testAdmin.GetAllSubscriptionGroup(context.Background(), brokerAddr, 3*time.Second)
	if err != nil {
		fmt.Println("GetAllSubscriptionGroup error:", err.Error())
	}
	fmt.Println(result.SubscriptionGroupTable)

	err = testAdmin.Close()
	if err != nil {
		fmt.Printf("Shutdown admin error: %s", err.Error())
	}
}
