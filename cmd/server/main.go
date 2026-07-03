package main

import (
	"log"
	"net"

	"mini-rpc/internal/types"
	"mini-rpc/pkg/rpc"
)

func main() {
	server := rpc.NewServer()

	// 注册一个加法函数
	server.RegisterFunction(types.Function{
		Name: "Add",
		Call: func(args ...interface{}) ([]interface{}, error) {
			a := args[0].(int)
			b := args[1].(int)
			return []interface{}{a + b}, nil
		},
	})

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("[cmd/server]启动监听失败:%v", err)
	}
	log.Println("[cmd/server]mini-rpc 服务启动, 端口:8080")
	server.Start(listener)
}
