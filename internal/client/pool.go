package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"

	"mini-rpc/pkg/registry"
)

const (
	RoundRobin = iota
	LeastConnections
	Random
)

type Pool struct {
	reg         registry.RegistryCenter
	serviceName string
	addrs       []string
	pools       map[string][]*RPCClient
	size        int
	addrIndex   int
	connIndex   map[string]int // 每个地址独立的连接轮询下标
	mu          sync.Mutex
}

func NewPool(size int, addr string) *Pool {
	return NewPoolForService(size, "", addr)
}

// NewPoolForService 创建绑定单个服务的连接池。
// serviceName 非空时，CallAsync 会拒绝调用其他服务的方法。
func NewPoolForService(size int, serviceName, addr string) *Pool {
	if size <= 0 {
		size = 1
	}
	p := &Pool{
		serviceName: serviceName,
		addrs:       []string{addr},
		pools:       make(map[string][]*RPCClient),
		size:        size,
		connIndex:   make(map[string]int),
	}
	pools := make([]*RPCClient, size)
	for i := range size {
		pools[i] = NewRPCClientWithAddr(addr)
	}
	p.pools[addr] = pools
	return p
}

func NewPoolTLS(size int, addr string, tlsConfig *tls.Config) *Pool {
	p := NewPool(size, addr)
	for _, conn := range p.pools[addr] {
		conn.tlsConfig = tlsConfig
	}
	return p
}

func NewPoolWithRegistry(size int, serviceName string, reg registry.RegistryCenter) *Pool {
	if size <= 0 {
		size = 1
	}
	p := &Pool{
		reg:         reg,
		serviceName: serviceName,
		addrs:       make([]string, 0),
		pools:       make(map[string][]*RPCClient),
		size:        size,
		connIndex:   make(map[string]int),
	}
	p.refresh()
	return p
}

func (p *Pool) refresh() {
	if p.reg == nil {
		log.Printf("[pool.go]registry 为 nil")
		return
	}
	if p.serviceName == "" {
		log.Printf("[pool.go]serviceName 为空，跳过服务发现")
		return
	}

	addrs, ok := p.reg.Discover(p.serviceName)
	if !ok {
		log.Printf("[pool.go]服务发现失败: %s", p.serviceName)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.connIndex == nil {
		p.connIndex = make(map[string]int)
	}

	newAddrs := make(map[string]bool)
	for _, addr := range addrs {
		newAddrs[addr] = true
		if _, exists := p.pools[addr]; !exists {
			n := p.size
			if n <= 0 {
				n = 1
			}
			conns := make([]*RPCClient, n)
			for i := range conns {
				conns[i] = NewRPCClientWithAddr(addr)
			}
			p.pools[addr] = conns
			p.connIndex[addr] = 0
		}
	}

	for _, addr := range p.addrs {
		if !newAddrs[addr] {
			delete(p.pools, addr)
			delete(p.connIndex, addr)
		}
	}

	p.addrs = addrs
	log.Printf("[pool.go]发现服务%s地址:%v", p.serviceName, addrs)
}

func (p *Pool) pickAddr(mode int) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.addrs) == 0 {
		return ""
	}

	switch mode {
	case RoundRobin:
		addr := p.addrs[p.addrIndex%len(p.addrs)]
		p.addrIndex++
		return addr
	case LeastConnections:
		minAddr := p.addrs[0]
		var minCount int64 = -1
		for _, addr := range p.addrs {
			var total int64
			for _, conn := range p.pools[addr] {
				total += conn.inflight.Load()
			}
			if minCount == -1 || total < minCount {
				minCount = total
				minAddr = addr
			}
		}
		return minAddr
	case Random:
		return p.addrs[rand.IntN(len(p.addrs))]
	}
	return p.addrs[0]
}

func (p *Pool) pickConn(addr string, mode int) *RPCClient {
	p.mu.Lock()
	defer p.mu.Unlock()

	conns := p.pools[addr]
	if len(conns) == 0 {
		return nil
	}

	switch mode {
	case RoundRobin:
		if p.connIndex == nil {
			p.connIndex = make(map[string]int)
		}
		idx := p.connIndex[addr]
		conn := conns[idx%len(conns)]
		p.connIndex[addr] = idx + 1
		return conn
	case LeastConnections:
		min := conns[0]
		for _, conn := range conns[1:] {
			if conn.inflight.Load() < min.inflight.Load() {
				min = conn
			}
		}
		return min
	case Random:
		return conns[rand.IntN(len(conns))]
	}
	return conns[0]
}

// CallAsync 发起一次异步调用。
// 返回 (*Future, error)：地址不可用 / 发送失败时 future=nil, err 非 nil。
// 调用方应用 ctx 控制超时：future.Get(ctx)。
func (p *Pool) CallAsync(ctx context.Context, funcname string, mode int, body []byte) (*Future, error) {
	if p.serviceName != "" && !strings.HasPrefix(funcname, p.serviceName+".") {
		return nil, fmt.Errorf("[pool.go]服务不匹配: 连接池服务=%s, 调用=%s", p.serviceName, funcname)
	}

	addr := p.pickAddr(mode)
	if addr == "" {
		return nil, errors.New("[pool.go]没有可用服务地址")
	}
	conn := p.pickConn(addr, mode)
	if conn == nil {
		return nil, fmt.Errorf("[pool.go]地址%s没有可用连接", addr)
	}
	return conn.CallAsync(ctx, funcname, body)
}
