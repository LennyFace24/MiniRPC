package client

import (
	"sync"
	"testing"
)

type mutableRegistry struct {
	mu       sync.Mutex
	services map[string][]string
}

func (r *mutableRegistry) Register(string, string) {}

func (r *mutableRegistry) Discover(serviceName string) ([]string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	addrs, ok := r.services[serviceName]
	return append([]string(nil), addrs...), ok
}

func (r *mutableRegistry) set(serviceName string, addrs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[serviceName] = addrs
}

func isClientClosed(c *RPCClient) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// TestPool_RefreshClosesRemovedClients 验证地址下线时旧连接会被关闭。
func TestPool_RefreshClosesRemovedClients(t *testing.T) {
	reg := &mutableRegistry{services: map[string][]string{
		"Calculator": {"A:8080"},
	}}
	pool := NewPoolWithRegistry(1, "Calculator", reg)
	defer pool.Close()

	old := pool.pools["A:8080"][0]
	reg.set("Calculator", []string{"B:9090"})
	pool.refresh()

	if _, ok := pool.pools["A:8080"]; ok {
		t.Fatal("旧地址没有被移除")
	}
	if len(pool.pools["B:9090"]) != 1 {
		t.Fatal("新地址没有被加入")
	}
	if !isClientClosed(old) {
		t.Fatal("旧地址的连接没有被关闭")
	}
}

// TestPool_Close 验证 Close 会关闭所有连接，并且可重复调用。
func TestPool_Close(t *testing.T) {
	pool := NewPool(2, "A:8080")
	conns := pool.pools["A:8080"]

	if err := pool.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	for i, conn := range conns {
		if !isClientClosed(conn) {
			t.Fatalf("连接%d没有被关闭", i)
		}
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("重复 Close 失败: %v", err)
	}
}
