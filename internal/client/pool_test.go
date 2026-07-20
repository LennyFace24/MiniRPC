package client

import (
	"testing"
)

func TestRoundRobin_Distribution(t *testing.T) {
	pool := NewPool(3, "localhost:8080")
	counts := make(map[int]int)

	for i := 0; i < 9; i++ {
		conn := pool.pickConn("localhost:8080", RoundRobin)
		found := false
		for j, c := range pool.pools["localhost:8080"] {
			if c == conn {
				counts[j]++
				found = true
				break
			}
		}
		if !found {
			t.Fatal("返回了不在连接池中的连接")
		}
	}

	for i := 0; i < 3; i++ {
		if counts[i] != 3 {
			t.Fatalf("连接%d: 期望3次, 得到%d次", i, counts[i])
		}
	}
}

func TestRandom_Distribution(t *testing.T) {
	pool := NewPool(5, "localhost:8080")
	counts := make(map[int]int)
	total := 1000

	for i := 0; i < total; i++ {
		conn := pool.pickConn("localhost:8080", Random)
		for j, c := range pool.pools["localhost:8080"] {
			if c == conn {
				counts[j]++
				break
			}
		}
	}

	// 每个连接平均 200 次，允许 ±40% 偏差
	for i := 0; i < 5; i++ {
		if counts[i] < 120 || counts[i] > 280 {
			t.Fatalf("连接%d: 期望~200次, 得到%d次", i, counts[i])
		}
	}
}

func TestLeastConnections_SelectsLeastBusy(t *testing.T) {
	pool := NewPool(3, "localhost:8080")
	conns := pool.pools["localhost:8080"]

	// 给连接0加2个待处理请求
	conns[0].pending[1] = make(chan *RPCResult, 1)
	conns[0].pending[2] = make(chan *RPCResult, 1)

	// 给连接1加1个待处理请求
	conns[1].pending[3] = make(chan *RPCResult, 1)

	// 应该选连接2（0个待处理）
	selected := pool.pickConn("localhost:8080", LeastConnections)
	if selected != conns[2] {
		t.Fatal("LeastConnections应选pending最少的连接")
	}
}

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
		t.Fatalf("RoundRobin地址分布不均匀: %v", counts)
	}
}

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

func TestNewPoolTLS(t *testing.T) {
	pool := NewPool(2, "localhost:443")
	if len(pool.pools["localhost:443"]) != 2 {
		t.Fatalf("期望2个连接, 得到%d", len(pool.pools["localhost:443"]))
	}
}