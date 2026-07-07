package main

import (
	"fmt"
	"log"

	"mini-rpc/pkg/rpc"
)

func main() {
	client := rpc.NewClient()

	future := client.CallAsync("Add", 3, 5)
	results, err := future.Get()
	if err != nil {
		log.Fatalf("[cmd/client]调用失败:%v", err)
	}

	fmt.Printf("[cmd/client]计算结果: %v\n", results[0])
}
