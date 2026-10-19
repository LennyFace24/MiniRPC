package main

import (
	"context"
	"flag"
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
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	server := rpc.NewServer()
	calc.RegisterCalculatorServer(server, &CalcServer{})
	server.Use(middleware.Logger)
	server.Use(middleware.Recovery)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("[cmd/server]启动监听失败:%v", err)
	}
	log.Printf("[cmd/server]mini-rpc 服务启动, 地址:%s", *addr)
	if err := server.Start(listener); err != nil {
		log.Fatalf("[cmd/server]服务退出:%v", err)
	}
}
