package utils

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	fastKeyReg   = regexp.MustCompile(`(\{|\,)(\d+)\:`)
	numberKeyReg = regexp.MustCompile(`\d+`)
)

// JavaFastJsonConvert covert java fastjson for golang
// `{"brokerAddrTable":{"rocketmq-fce78b93-1":{"brokerAddrs":{0:"10.10.88.243:27622",2:"10.10.88.243:27328",3:"10.10.88.243:27664"},"brokerName":"rocketmq-fce78b93-1","cluster":"rocketmq-fce78b93"},"rocketmq-fce78b93-0":{"brokerAddrs":{0:"10.10.88.243:27562",2:"10.10.88.243:27565",3:"10.10.88.243:27902"},"brokerName":"rocketmq-fce78b93-0","cluster":"rocketmq-fce78b93"}},"clusterAddrTable":{"rocketmq-fce78b93":["rocketmq-fce78b93-1","rocketmq-fce78b93-0"]}}`
func JavaFastJsonConvert(fjsonStr string) string {
	if fjsonStr == "" {
		return ""
	}
	return fastKeyReg.ReplaceAllStringFunc(fjsonStr, func(s string) string {
		matches := numberKeyReg.FindStringSubmatch(s)
		if len(matches) != 1 {
			return s
		}
		return strings.ReplaceAll(s, matches[0], fmt.Sprintf(`"%s"`, matches[0]))
	})
}
