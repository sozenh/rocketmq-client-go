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
	nameSrvAddr := []string{"10.10.88.243:27852"}

	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: "rocketAdmin",
			SecretKey: "BHZ1P97DUI4JEO1XU2ZLCE9OG6JV",
		}),
	)
	// res, _ := testAdmin.FetchClusterInfo(context.Background())
	// fmt.Println(res)

	ures, err := testAdmin.UpdateBrokerConfig(context.Background(), "fileReservedTime=16")

	fmt.Println(ures)
	fmt.Println(err)

	return
	brokerAddr := "10.10.88.243:123"
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
