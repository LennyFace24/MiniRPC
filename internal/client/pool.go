package client

import (
	"context"
	"crypto/tls"
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
	reg  registry.RegistryCenter
	addr []string
	conn []*RPCClient
	where int
	mu    sync.Mutex
}

func NewPool(size int, addr string) *Pool {
	p := &Pool{
		addr:  []string{addr},
		conn:  make([]*RPCClient, 0, size),
		where: 0,
		mu:    sync.Mutex{},
	}
	for i := 0; i < size; i++ {
		conn := NewRPCClient()
		p.conn = append(p.conn, conn)
	}
	return p
}

func NewPoolTLS(size int, addr string, tlsConfig *tls.Config) *Pool {
	p := NewPool(size, addr)
	for _, conn := range p.conn {
		conn.tlsConfig = tlsConfig
	}
	return p
}

func NewPoolWithRegistry(size int, reg registry.RegistryCenter) *Pool {
	p := &Pool{
		reg:   reg,
		addr:  make([]string, 0),
		conn:  make([]*RPCClient, 0, size),
		where: 0,
		mu:    sync.Mutex{},
	}
	for i := 0; i < size; i++ {
		conn := NewRPCClient()
		p.conn = append(p.conn, conn)
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
	p.addr = addrs
	log.Printf("[pool.go]发现服务地址:%v", addrs)
}

func (p *Pool) NewConn(addr string) *RPCClient {
	p.mu.Lock()
	defer p.mu.Unlock()
	conn := NewRPCClient()
	p.conn = append(p.conn, conn)
	return conn
}

func (p *Pool) CallAsync(ctx context.Context, funcname string, mode int, body []byte) *Future {
	if mode == RoundRobin {
		return p.roundRobin().CallAsync(ctx, funcname, body)
	}
	if mode == LeastConnections {
		return p.leastConnections().CallAsync(ctx, funcname, body)
	}
	if mode == Random {
		return p.random().CallAsync(ctx, funcname, body)
	}
	log.Default().Printf("[pool.go]负载均衡策略错误:%v", mode)
	return nil
}

func (p *Pool) roundRobin() *RPCClient {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.where >= len(p.conn) {
		p.where = 0
	}
	conn := p.conn[p.where]
	p.where++
	return conn
}

func (p *Pool) leastConnections() *RPCClient {
	p.mu.Lock()
	defer p.mu.Unlock()
	min := p.conn[0]
	for _, conn := range p.conn[1:] {
		if len(conn.pending) < len(min.pending) {
			min = conn
		}
	}
	return min
}

func (p *Pool) random() *RPCClient {
	return p.conn[rand.Intn(len(p.conn))]
}