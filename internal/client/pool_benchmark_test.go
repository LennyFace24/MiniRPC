package client

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"

	"mini-rpc/internal/server"
)

// benchPoolHandler 用于 benchmark 的服务实现（不依赖 calc，避免 import cycle）
type benchPoolHandler struct{}

func (s *benchPoolHandler) Add(body []byte) ([]byte, error) {
	if len(body) != 8 {
		return nil, nil
	}
	a := int32(binary.BigEndian.Uint32(body[0:4]))
	b := int32(binary.BigEndian.Uint32(body[4:8]))
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(a+b))
	return out, nil
}

// startBenchPoolServer 启动真实 TCP server 用于 pool benchmark
func startBenchPoolServer(b testing.TB) (string, func()) {
	b.Helper()
	srv := server.NewRPCServer()
	srv.RegisterService("Calc", &benchPoolHandler{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Listen失败: %v", err)
	}
	go srv.Start(ln)
	return ln.Addr().String(), func() { ln.Close() }
}

// benchBody 预构造的 Add 请求 body
var benchBody = makeAddBody(3, 5)

// mustCallAsync 是 benchmark 辅助：失败直接 b.Fatalf，避免重复样板代码
func mustCallAsync(b *testing.B, pool *Pool, ctx context.Context, mode int) []byte {
	future, err := pool.CallAsync(ctx, "Calc.Add", mode, benchBody)
	if err != nil {
		b.Fatalf("CallAsync 失败: %v", err)
	}
	ret, err := future.Get(ctx)
	if err != nil {
		b.Fatalf("调用失败: %v", err)
	}
	return ret
}

// BenchmarkPool_SingleConn_Serial 单连接串行调用
func BenchmarkPool_SingleConn_Serial(b *testing.B) {
	b.ReportAllocs()
	addr, stop := startBenchPoolServer(b)
	defer stop()

	pool := NewPool(1, addr)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustCallAsync(b, pool, ctx, RoundRobin)
	}
}

// BenchmarkPool_MultiConn_Serial 多连接串行调用
func BenchmarkPool_MultiConn_Serial(b *testing.B) {
	b.ReportAllocs()
	addr, stop := startBenchPoolServer(b)
	defer stop()

	pool := NewPool(4, addr)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustCallAsync(b, pool, ctx, RoundRobin)
	}
}

// BenchmarkPool_MultiConn_Parallel 多连接并发调用
// 这是连接池存在的核心价值场景
func BenchmarkPool_MultiConn_Parallel(b *testing.B) {
	b.ReportAllocs()
	addr, stop := startBenchPoolServer(b)
	defer stop()

	pool := NewPool(8, addr)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mustCallAsync(b, pool, ctx, RoundRobin)
		}
	})
}

// BenchmarkPool_Strategies 对比三种负载均衡策略在并发下的性能
func BenchmarkPool_Strategies(b *testing.B) {
	addr, stop := startBenchPoolServer(b)
	defer stop()

	ctx := context.Background()

	b.Run("RoundRobin", func(b *testing.B) {
		b.ReportAllocs()
		pool := NewPool(4, addr)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mustCallAsync(b, pool, ctx, RoundRobin)
			}
		})
	})

	b.Run("LeastConnections", func(b *testing.B) {
		b.ReportAllocs()
		pool := NewPool(4, addr)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mustCallAsync(b, pool, ctx, LeastConnections)
			}
		})
	})

	b.Run("Random", func(b *testing.B) {
		b.ReportAllocs()
		pool := NewPool(4, addr)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mustCallAsync(b, pool, ctx, Random)
			}
		})
	})
}

// BenchmarkPool_CallAsync_NoIO 用真实 TCP 测纯调度开销（单连接基线）
func BenchmarkPool_CallAsync_NoIO(b *testing.B) {
	b.ReportAllocs()
	addr, stop := startBenchPoolServer(b)
	defer stop()

	pool := NewPool(1, addr)
	ctx := context.Background()

	// 预热连接
	mustCallAsync(b, pool, ctx, RoundRobin)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustCallAsync(b, pool, ctx, RoundRobin)
	}
}

// BenchmarkPool_MultiAddr_Parallel 多地址并发
func BenchmarkPool_MultiAddr_Parallel(b *testing.B) {
	b.ReportAllocs()

	var addrs []string
	var stops []func()
	for i := 0; i < 3; i++ {
		addr, stop := startBenchPoolServer(b)
		addrs = append(addrs, addr)
		stops = append(stops, stop)
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()

	pool := &Pool{
		addrs: addrs,
		pools: make(map[string][]*RPCClient),
	}
	for _, addr := range addrs {
		conns := make([]*RPCClient, 2)
		for j := range conns {
			conns[j] = NewRPCClientWithAddr(addr)
		}
		pool.pools[addr] = conns
	}

	ctx := context.Background()

	// 预热所有连接
	wg := &sync.WaitGroup{}
	for _, addr := range addrs {
		for _, c := range pool.pools[addr] {
			wg.Add(1)
			go func(c *RPCClient) {
				defer wg.Done()
				future, err := c.CallAsync(ctx, "Calc.Add", benchBody)
				if err != nil {
					b.Errorf("预热失败: %v", err)
					return
				}
				if _, err := future.Get(ctx); err != nil {
					b.Errorf("预热失败: %v", err)
				}
			}(c)
		}
	}
	wg.Wait()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mustCallAsync(b, pool, ctx, RoundRobin)
		}
	})
}
