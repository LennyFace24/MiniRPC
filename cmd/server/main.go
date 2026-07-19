package main

import (
	"context"
	"log"
	"net"

	calc "mini-rpc/internal/codec/proto/calc"
	"mini-rpc/internal/middleware"
	"mini-rpc/pkg/rpc"
)

type CalcServer struct{}

func (s *CalcServer) Add(ctx context.Context, req *calc.CalcReq) (*calc.CalcRsp, error) {
	return &calc.CalcRsp{Result: req.A + req.B}, nil
}

func main() {
	server := rpc.NewServer()
	calc.RegisterCalculatorServer(server, &CalcServer{})
	server.Use(middleware.Logger)
	server.Use(middleware.Recovery)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("[cmd/server]启动监听失败:%v", err)
	}
	log.Println("[cmd/server]mini-rpc 服务启动, 端口:8080")
	server.Start(listener)

}
