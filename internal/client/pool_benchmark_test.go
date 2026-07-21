package client

import (
	"context"
	"net"
	"testing"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/server"
	"google.golang.org/protobuf/proto"
)

type benchPoolHandler struct{}

func (s *benchPoolHandler) Add(body []byte) ([]byte, error) {
	return proto.Marshal(&framepb.MessageResponse{
		Returns: []byte{8, 8},
		Error:   "",
	})
}

func startBenchServer(b *testing.B, addr string) {
	srv := server.NewRPCServer()
	srv.RegisterService("Calc", &benchPoolHandler{})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		b.Fatalf("启动bench服务失败:%v", err)
	}
	go srv.Start(ln)
}

func BenchmarkPoolSingleConn(b *testing.B) {
	startBenchServer(b, "127.0.0.1:0")

	pool := &Pool{
		addrs: []string{"127.0.0.1:0"},
		pools: map[string][]*RPCClient{
			"127.0.0.1:0": {NewRPCClient()},
		},
	}

	ctx := context.Background()
	body, _ := proto.Marshal(&framepb.MessageRequest{FuncName: "Calc.Add", Args: []byte{8, 3, 16, 5}})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		future := pool.CallAsync(ctx, "Calc.Add", RoundRobin, body)
		future.Get()
	}
}

func BenchmarkPoolMultiConn(b *testing.B) {
	startBenchServer(b, "127.0.0.1:0")

	pool := &Pool{
		addrs: []string{"127.0.0.1:0"},
		pools: map[string][]*RPCClient{
			"127.0.0.1:0": {NewRPCClient(), NewRPCClient(), NewRPCClient()},
		},
	}

	ctx := context.Background()
	body, _ := proto.Marshal(&framepb.MessageRequest{FuncName: "Calc.Add", Args: []byte{8, 3, 16, 5}})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		future := pool.CallAsync(ctx, "Calc.Add", RoundRobin, body)
		future.Get()
	}
}

func BenchmarkPoolRoundRobin(b *testing.B) {
	benchPool(b, RoundRobin)
}

func BenchmarkPoolLeastConnections(b *testing.B) {
	benchPool(b, LeastConnections)
}

func BenchmarkPoolRandom(b *testing.B) {
	benchPool(b, Random)
}

func benchPool(b *testing.B, mode int) {
	startBenchServer(b, "127.0.0.1:0")

	pool := &Pool{
		addrs: []string{"127.0.0.1:0"},
		pools: map[string][]*RPCClient{
			"127.0.0.1:0": {NewRPCClient(), NewRPCClient(), NewRPCClient()},
		},
	}

	ctx := context.Background()
	body, _ := proto.Marshal(&framepb.MessageRequest{FuncName: "Calc.Add", Args: []byte{8, 3, 16, 5}})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		future := pool.CallAsync(ctx, "Calc.Add", mode, body)
		future.Get()
	}
}