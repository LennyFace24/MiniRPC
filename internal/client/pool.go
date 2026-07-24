package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"mini-rpc/pkg/registry"
)

const (
	RoundRobin = iota
	LeastConnections
	Random
)

type Pool struct {
	reg       registry.RegistryCenter
	addrs     []string
	pools     map[string][]*RPCClient
	addrIndex int
	mu        sync.Mutex
}

func NewPool(size int, addr string) *Pool {
	p := &Pool{
		addrs: []string{addr},
		pools: make(map[string][]*RPCClient),
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

func NewPoolWithRegistry(size int, reg registry.RegistryCenter) *Pool {
	p := &Pool{
		reg:   reg,
		addrs: make([]string, 0),
		pools: make(map[string][]*RPCClient),
	}
	p.refresh()
	return p
}

func (p *Pool) refresh() {
	addrs, ok := p.reg.Discover("Calculator")
	if !ok {
		log.Printf("[pool.go]服务发现失败")
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	old := make(map[string]bool)
	for _, addr := range p.addrs {
		old[addr] = true
	}

	newAddrs := make(map[string]bool)
	for _, addr := range addrs {
		newAddrs[addr] = true
		if _, exists := p.pools[addr]; !exists {
			p.pools[addr] = []*RPCClient{NewRPCClientWithAddr(addr)}
		}
	}

	for _, addr := range p.addrs {
		if !newAddrs[addr] {
			delete(p.pools, addr)
		}
	}

	p.addrs = addrs
	log.Printf("[pool.go]发现服务地址:%v", addrs)
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
		minCount := -1
		for _, addr := range p.addrs {
			total := 0
			for _, conn := range p.pools[addr] {
				total += len(conn.pending)
			}
			if minCount == -1 || total < minCount {
				minCount = total
				minAddr = addr
			}
		}
		return minAddr
	case Random:
		return p.addrs[rand.Intn(len(p.addrs))]
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
		conn := conns[p.addrIndex%len(conns)]
		p.addrIndex++
		return conn
	case LeastConnections:
		min := conns[0]
		for _, conn := range conns[1:] {
			if len(conn.pending) < len(min.pending) {
				min = conn
			}
		}
		return min
	case Random:
		return conns[rand.Intn(len(conns))]
	}
	return conns[0]
}

// CallAsync 发起一次异步调用。
// 返回 (*Future, error)：地址不可用 / 发送失败时 future=nil, err 非 nil。
// 调用方应用 ctx 控制超时：future.Get(ctx)。
func (p *Pool) CallAsync(ctx context.Context, funcname string, mode int, body []byte) (*Future, error) {
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
