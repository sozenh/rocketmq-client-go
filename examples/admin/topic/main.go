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
	topic := "newOne"
	//clusterName := "DefaultCluster"
	nameSrvAddr := []string{"10.10.88.152:9876"}
	brokerAddr := "10.10.88.152:10911"

	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: "rocketmq2",
			SecretKey: "12345678",
		}),
	)

	// get all topic config
	res, err := testAdmin.FetchAllTopicConfig(context.Background(), brokerAddr)
	fmt.Println("all topic config: ", res)

	//create topic
	err = testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate(topic),
		admin.WithBrokerAddrCreate(brokerAddr),
	)
	if err != nil {
		fmt.Println("Create topic error:", err.Error())
	}

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
