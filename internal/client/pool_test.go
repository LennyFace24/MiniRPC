package client

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mini-rpc/internal/server"
)

// testPoolHandler 测试用的服务（不依赖 calc，避免 import cycle）
type testPoolHandler struct{}

func (s *testPoolHandler) Add(body []byte) ([]byte, error) {
	if len(body) != 8 {
		return nil, nil
	}
	a := int32(binary.BigEndian.Uint32(body[0:4]))
	b := int32(binary.BigEndian.Uint32(body[4:8]))
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(a+b))
	return out, nil
}

// makeAddBody 构造两个 int32 的 big-endian body
func makeAddBody(a, b int32) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint32(out[0:4], uint32(a))
	binary.BigEndian.PutUint32(out[4:8], uint32(b))
	return out
}

// parseAddResult 解析 int32 big-endian 返回值
func parseAddResult(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

// startTestServer 启动真实 TCP server 用于集成测试
func startTestServer(t *testing.T) (string, func()) {
	t.Helper()
	srv := server.NewRPCServer()
	srv.RegisterService("Calc", &testPoolHandler{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen失败: %v", err)
	}
	go srv.Start(ln)
	return ln.Addr().String(), func() { ln.Close() }
}

// TestRoundRobin_Distribution 验证 RoundRobin 在单地址多连接下的均匀分布
func TestRoundRobin_Distribution(t *testing.T) {
	pool := NewPool(3, "localhost:8080")
	counts := make(map[*RPCClient]int)

	for i := 0; i < 9; i++ {
		conn := pool.pickConn("localhost:8080", RoundRobin)
		counts[conn]++
	}

	if len(counts) != 3 {
		t.Fatalf("期望3个不同连接, 得到%d", len(counts))
	}
	for conn, n := range counts {
		if n != 3 {
			t.Fatalf("连接%p: 期望3次, 得到%d次", conn, n)
		}
	}
}

// TestRandom_Distribution 验证 Random 的统计均匀性
func TestRandom_Distribution(t *testing.T) {
	pool := NewPool(5, "localhost:8080")
	counts := make(map[*RPCClient]int)
	total := 1000

	for i := 0; i < total; i++ {
		conn := pool.pickConn("localhost:8080", Random)
		counts[conn]++
	}

	if len(counts) != 5 {
		t.Fatalf("期望5个不同连接, 得到%d", len(counts))
	}
	for conn, n := range counts {
		if n < 120 || n > 280 {
			t.Fatalf("连接%p: 期望~200次, 得到%d次", conn, n)
		}
	}
}

// TestLeastConnections_SelectsLeastBusy 验证 LeastConnections 选 pending 最少的连接
func TestLeastConnections_SelectsLeastBusy(t *testing.T) {
	pool := NewPool(3, "localhost:8080")
	conns := pool.pools["localhost:8080"]

	conns[0].pending[1] = make(chan *RPCResult, 1)
	conns[0].pending[2] = make(chan *RPCResult, 1)
	conns[1].pending[3] = make(chan *RPCResult, 1)

	selected := pool.pickConn("localhost:8080", LeastConnections)
	if selected != conns[2] {
		t.Fatal("LeastConnections 应选 pending 最少的连接")
	}
}

// TestPool_MultiAddrDistribution 验证多地址 RoundRobin 分布均匀
func TestPool_MultiAddrDistribution(t *testing.T) {
	pool := &Pool{
		addrs: []string{"A:8080", "B:8080", "C:8080"},
		pools: map[string][]*RPCClient{
			"A:8080": {NewRPCClient()},
			"B:8080": {NewRPCClient()},
			"C:8080": {NewRPCClient()},
		},
	}

	counts := make(map[string]int)
	for i := 0; i < 6; i++ {
		addr := pool.pickAddr(RoundRobin)
		counts[addr]++
	}

	if counts["A:8080"] != 2 || counts["B:8080"] != 2 || counts["C:8080"] != 2 {
		t.Fatalf("RoundRobin 地址分布不均匀: %v", counts)
	}
}

// TestEmptyPool 验证空地址列表
func TestEmptyPool(t *testing.T) {
	pool := &Pool{
		addrs: []string{},
		pools: map[string][]*RPCClient{},
	}

	addr := pool.pickAddr(RoundRobin)
	if addr != "" {
		t.Fatal("空地址列表应返回空字符串")
	}
}

// TestNewPoolSize 验证 NewPool 创建指定数量的连接
func TestNewPoolSize(t *testing.T) {
	pool := NewPool(2, "localhost:443")
	if len(pool.pools["localhost:443"]) != 2 {
		t.Fatalf("期望2个连接, 得到%d", len(pool.pools["localhost:443"]))
	}
}

// TestNewPoolWithAddr 验证 NewPool 创建的客户端携带地址
func TestNewPoolWithAddr(t *testing.T) {
	addr := "localhost:9999"
	pool := NewPool(3, addr)
	for i, c := range pool.pools[addr] {
		if c.addr != addr {
			t.Fatalf("连接%d的地址: 期望%s, 得到%s", i, addr, c.addr)
		}
	}
}

// --- 端到端集成测试 ---

// TestPool_EndToEnd_CallAsync 验证连接池端到端调用
func TestPool_EndToEnd_CallAsync(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	pool := NewPool(2, addr)
	ctx := context.Background()

	future, err := pool.CallAsync(ctx, "Calc.Add", RoundRobin, makeAddBody(10, 20))
	if err != nil {
		t.Fatalf("CallAsync 失败: %v", err)
	}
	if future == nil {
		t.Fatal("CallAsync 返回 nil future")
	}

	ret, err := future.Get(ctx)
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}

	if got := parseAddResult(ret); got != 30 {
		t.Fatalf("Result: 期望30, 得到%d", got)
	}
}

// TestPool_EndToEnd_MultipleRequests 验证连接池多次调用稳定性
func TestPool_EndToEnd_MultipleRequests(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	pool := NewPool(3, addr)
	ctx := context.Background()

	for i := int32(0); i < 10; i++ {
		future, err := pool.CallAsync(ctx, "Calc.Add", RoundRobin, makeAddBody(i, i))
		if err != nil {
			t.Fatalf("第%d次 CallAsync 失败: %v", i, err)
		}
		ret, err := future.Get(ctx)
		if err != nil {
			t.Fatalf("第%d次调用失败: %v", i, err)
		}
		if got := parseAddResult(ret); got != i+i {
			t.Fatalf("第%d次Result: 期望%d, 得到%d", i, i+i, got)
		}
	}
}

// TestPool_EndToEnd_Concurrent 验证连接池并发调用正确性
func TestPool_EndToEnd_Concurrent(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	pool := NewPool(4, addr)
	ctx := context.Background()

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	var failures int32

	for i := int32(0); i < N; i++ {
		go func(a, b int32) {
			defer wg.Done()
			future, err := pool.CallAsync(ctx, "Calc.Add", RoundRobin, makeAddBody(a, b))
			if err != nil {
				t.Errorf("CallAsync 失败: %v", err)
				atomic.AddInt32(&failures, 1)
				return
			}
			ret, err := future.Get(ctx)
			if err != nil {
				t.Errorf("调用失败: %v", err)
				atomic.AddInt32(&failures, 1)
				return
			}
			if got := parseAddResult(ret); got != a+b {
				t.Errorf("Result: 期望%d, 得到%d", a+b, got)
				atomic.AddInt32(&failures, 1)
			}
		}(i, i*2)
	}
	wg.Wait()

	if failures > 0 {
		t.Fatalf("%d 个并发请求失败", failures)
	}
}

// TestPool_EndToEnd_Strategies 验证三种策略都能正常调用
func TestPool_EndToEnd_Strategies(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	ctx := context.Background()
	body := makeAddBody(5, 7)

	strategies := []struct {
		name string
		mode int
	}{
		{"RoundRobin", RoundRobin},
		{"LeastConnections", LeastConnections},
		{"Random", Random},
	}

	for _, s := range strategies {
		t.Run(s.name, func(t *testing.T) {
			pool := NewPool(3, addr)
			future, err := pool.CallAsync(ctx, "Calc.Add", s.mode, body)
			if err != nil {
				t.Fatalf("CallAsync 失败: %v", err)
			}
			if future == nil {
				t.Fatal("CallAsync 返回 nil")
			}
			ret, err := future.Get(ctx)
			if err != nil {
				t.Fatalf("调用失败: %v", err)
			}
			if got := parseAddResult(ret); got != 12 {
				t.Fatalf("Result: 期望12, 得到%d", got)
			}
		})
	}
}

// TestPool_EndToEnd_WithTimeout 验证带超时的调用
func TestPool_EndToEnd_WithTimeout(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	pool := NewPool(1, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	future, err := pool.CallAsync(ctx, "Calc.Add", RoundRobin, makeAddBody(3, 5))
	if err != nil {
		t.Fatalf("CallAsync 失败: %v", err)
	}
	ret, err := future.Get(ctx)
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}
	if got := parseAddResult(ret); got != 8 {
		t.Fatalf("Result: 期望8, 得到%d", got)
	}
}

// TestClient_Reconnect 验证连接断开后自动重连
func TestClient_Reconnect(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	c := NewRPCClientWithAddr(addr)
	ctx := context.Background()

	// 第一次调用：建立连接
	future, err := c.CallAsync(ctx, "Calc.Add", makeAddBody(1, 2))
	if err != nil {
		t.Fatalf("首次 CallAsync 失败: %v", err)
	}
	ret, err := future.Get(ctx)
	if err != nil {
		t.Fatalf("首次调用失败: %v", err)
	}
	if got := parseAddResult(ret); got != 3 {
		t.Fatalf("首次 Result: 期望3, 得到%d", got)
	}

	// 强制关闭连接模拟断线
	c.triggerReconnect()

	// 第二次调用：应自动重连
	future, err = c.CallAsync(ctx, "Calc.Add", makeAddBody(4, 5))
	if err != nil {
		t.Fatalf("重连后 CallAsync 失败: %v", err)
	}
	ret, err = future.Get(ctx)
	if err != nil {
		t.Fatalf("重连后调用失败: %v", err)
	}
	if got := parseAddResult(ret); got != 9 {
		t.Fatalf("重连后 Result: 期望9, 得到%d", got)
	}
}

// TestClient_TimeoutActuallyWorks 验证 ctx 超时真正生效（不再永久阻塞）
func TestClient_TimeoutActuallyWorks(t *testing.T) {
	// 监听一个不会响应的"假"服务器：accept 后不读不写
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen失败: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// 故意不处理，让 client 等响应
			_ = conn
		}
	}()

	c := NewRPCClientWithAddr(ln.Addr().String())
	// 短超时：100ms
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	future, err := c.CallAsync(ctx, "Calc.Add", makeAddBody(1, 2))
	if err != nil {
		t.Fatalf("CallAsync 失败: %v", err)
	}

	// 必须在 ~100ms 内返回 ctx.Err()，而不是永久阻塞
	done := make(chan error, 1)
	go func() {
		_, err := future.Get(ctx)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("期望超时错误, 得到 nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("future.Get 永久阻塞，ctx 超时未生效")
	}
}
