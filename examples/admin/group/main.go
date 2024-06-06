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
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	//clusterName := "DefaultCluster"
	nameSrvAddr := []string{"10.10.88.243:26875"}
	// nameSrvAddr := []string{"10.10.88.152:9876"}
	// rocketmq-7b98698f-0 0:10.10.88.243:25035
	brokerAddr := "10.10.88.243:25035"
	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: "rocketAdmin",
			// AccessKey: "rocketmq2",
			SecretKey: "6DQLYI8Q0R7ZDVT6DLGAV1F2RHK9",
			// SecretKey: "12345678",
		}),
	)
	// res, _ := testAdmin.FetchClusterInfo(context.Background())
	// fmt.Println(res)

	// ures, err := testAdmin.UpdateBrokerConfig(context.Background(), "fileReservedTime", "16")
	// fmt.Println(ures)
	// fmt.Println(err)
	cfg := []admin.AclFuncOption{
		admin.WithAccessKey("gxltest0001"),
		admin.WithSecretKey("TtestPSHHSHHAJKSHKJAHSK"),
		admin.WithWhiteRemoteAddress(""),
		admin.WithDefaultTopicPerm("PUB|SUB"),
		admin.WithDefaultGroupPerm("PUB|SUB"),
		admin.WithAdmin("false"),
		admin.WithTopicPerms("topicA=DENY,topicB=PUB|SUB,topicC=SUB"),
		admin.WithGroupPerms("groupA=DENY,groupB=PUB|SUB,groupC=SUB"),
	}

	err = testAdmin.UpdateAclConfig(context.Background(), cfg...)
	fmt.Println(err)

	err = testAdmin.DeleteAclConfig(context.Background(), "gxltest0001")
	fmt.Println(err)

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
