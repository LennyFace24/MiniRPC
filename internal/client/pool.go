package client

import (
	"log"
	"math/rand"
	"sync"
)

// 负载均衡策略
const (
	RoundRobin = iota
	LeastConnections
	Random
)

// 连接池
type Pool struct {
	conn  []*RPCClient
	where int
	mu    sync.Mutex
}

func NewPool(size int, addr string) *Pool {

	p := &Pool{
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

func (p *Pool) NewConn(addr string) *RPCClient {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 创建新的连接
	conn := NewRPCClient()
	p.conn = append(p.conn, conn)
	return conn
}

func (p *Pool) CallAsync(funcname string, mode int, body []byte) *Future {
	if mode == RoundRobin {
		return p.roundRobin().CallAsync(funcname, body)
	}
	if mode == LeastConnections {
		return p.leastConnections().CallAsync(funcname, body)
	}
	if mode == Random {
		return p.random().CallAsync(funcname, body)
	}
	log.Default().Printf("[pool.go]负载均衡策略错误:%v", mode)
	return nil
}

func (p *Pool) roundRobin() *RPCClient {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 用where
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

	// 获取每个rpcclient的连接数，选出其中最少的
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
