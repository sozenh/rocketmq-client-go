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

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	topic := "mmmmmmbaaaaxxxxx"
	//clusterName := "DefaultCluster"
	nameSrvAddr := []string{"10.10.88.243:26875"}
	brokerAddr := "10.10.88.243:25011"
	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: "rocketAdmin",
			SecretKey: "6DQLYI8Q0R7ZDVT6DLGAV1F2RHK9",
		}),
	)

	//create topic
	err = testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate(topic),
		admin.WithOrder(true),
		admin.WithWriteQueueNums(8),
		admin.WithBrokerAddrCreate(brokerAddr),
	)
	if err != nil {
		fmt.Println("Create topic error:", err.Error())
	}

	// set order kvconfig
	err = testAdmin.PutOrderKVConfig(context.Background(), "mmmmmmbaaaaxxxxx", "rocketmq-2a597b8d-0:8")
	if err != nil {
		fmt.Println("set order kvconfig error:", err.Error())
	}
	// get all topic config
	res, err := testAdmin.FetchAllTopicConfig(context.Background(), brokerAddr)
	fmt.Println("all topic config: ", res)
	return
	// topic list
	result, err := testAdmin.FetchAllTopicList(context.Background())
	if err != nil {
		fmt.Println("FetchAllTopicList error:", err.Error())
	}
	fmt.Println("topic list", result.TopicList)

	// deletetopic
	// err = testAdmin.DeleteTopic(
	// 	context.Background(),
	// 	admin.WithTopicDelete(topic),
	// 	admin.WithBrokerAddrDelete(brokerAddr),
	// 	admin.WithNameSrvAddr(nameSrvAddr),
	// )
	// if err != nil {
	// 	fmt.Println("Delete topic error:", err.Error())
	// }

	err = testAdmin.Close()
	if err != nil {
		fmt.Printf("Shutdown admin error: %s", err.Error())
	}
}
