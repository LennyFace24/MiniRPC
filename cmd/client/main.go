package main

import (
	"context"
	"fmt"
	"log"

	calc "mini-rpc/internal/codec/proto/calc"
)

func main() {
	client := calc.NewCalculatorClient("localhost:8080")
	rsp, err := client.Add(context.Background(), &calc.CalcReq{A: 3, B: 5})
	if err != nil {
		log.Fatalf("[cmd/client]调用失败:%v", err)
	}
	fmt.Printf("[cmd/client]计算结果: %d\n", rsp.Result)
}