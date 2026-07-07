package main

import (
	"log"
	"net"

	"mini-rpc/internal/middleware"
	"mini-rpc/pkg/rpc"
)

type Add struct{}

func (a *Add) Add(x, y int) int {
	return x + y
}

func main() {
	server := rpc.NewServer()

	// 注册一个加法函数
	server.RegisterFunction(&Add{})
	// 注册中间件
	server.Use(middleware.Logger)
	server.Use(middleware.Recovery)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("[cmd/server]启动监听失败:%v", err)
	}
	log.Println("[cmd/server]mini-rpc 服务启动, 端口:8080")
	server.Start(listener)
}
