package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	calc "mini-rpc/internal/codec/proto/calc"
)

type StaticReg struct {
	once       sync.Once
	serviceMap map[string][]string // 每个服务对应的地址
}

func (r *StaticReg) Register(serviceName string, address string) {

}

// 服务发现,在服务列表更新时进行重新发现
func (r *StaticReg) Discover(serviceName string) ([]string, bool) {
	var addrs []string
	file, err := os.ReadFile("address.json")
	if err != nil {
		log.Fatalf("[cmd/client]读取配置文件失败:%v", err)
	}
	err = json.Unmarshal(file, &addrs)
	if err != nil {
		log.Fatalf("[cmd/client]解析配置文件失败:%v", err)
	}
	return addrs, true
}

func main() {
	client := calc.NewCalculatorClient("localhost:8080")
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(time.Second*1))
	rsp, err := client.Add(ctx, &calc.CalcReq{A: 3, B: 5})
	if err != nil {
		log.Fatalf("[cmd/client]调用失败:%v", err)
	}
	fmt.Printf("[cmd/client]计算结果: %d\n", rsp.Result)
}
