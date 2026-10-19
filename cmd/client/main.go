package main

import (
	"context"
	"fmt"
	"log"
	"time"

	calc "mini-rpc/internal/codec/proto/calc"
	"mini-rpc/pkg/registry"
)

func main() {
	client := calc.NewCalculatorClientWithRegistry(registry.NewFileRegistry("address.json"))
	defer client.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*1))
	defer cancel()
	rsp, err := client.Add(ctx, &calc.CalcReq{A: 3, B: 5})
	if err != nil {
		log.Fatalf("[cmd/client]调用失败:%v", err)
	}
	fmt.Printf("[cmd/client]计算结果: %d\n", rsp.Result)
}
