package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	calc "mini-rpc/internal/codec/proto/calc"
)

type StaticReg struct{}

func (r *StaticReg) Register(serviceName string, address string) {}

// 服务发现：address.json 按 serviceName 分组，避免把不同服务的地址混进同一个 Pool。
func (r *StaticReg) Discover(serviceName string) ([]string, bool) {
	var services map[string][]string
	file, err := os.ReadFile("address.json")
	if err != nil {
		log.Fatalf("[cmd/client]读取配置文件失败:%v", err)
	}
	if err := json.Unmarshal(file, &services); err != nil {
		log.Fatalf("[cmd/client]解析配置文件失败:%v", err)
	}
	addrs, ok := services[serviceName]
	return addrs, ok
}

func main() {
	client := calc.NewCalculatorClient("localhost:8080")
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*1))
	defer cancel()
	rsp, err := client.Add(ctx, &calc.CalcReq{A: 3, B: 5})
	if err != nil {
		log.Fatalf("[cmd/client]调用失败:%v", err)
	}
	fmt.Printf("[cmd/client]计算结果: %d\n", rsp.Result)
}
