package rpc

import (
	"crypto/tls"

	"mini-rpc/internal/client"
	"mini-rpc/internal/server"
	"mini-rpc/pkg/registry"
)

// 公共类型别名，让调用方不需要直接依赖 internal 包。
type (
	Server    = server.RPCServer
	Client    = client.RPCClient
	Pool      = client.Pool
	Future    = client.Future
	RPCResult = client.RPCResult
)

const (
	RoundRobin       = client.RoundRobin
	LeastConnections = client.LeastConnections
	Random           = client.Random
)

func NewServer() *Server {
	return server.NewRPCServer()
}

// NewClient 创建连接单个显式地址的客户端。
func NewClient(addr string) *Client {
	return client.NewRPCClientWithAddr(addr)
}

// NewClientWithRegistry 创建单连接池。
// registry 可能返回多个副本地址，因此返回 Pool 而不是单个 RPCClient。
func NewClientWithRegistry(serviceName string, reg registry.RegistryCenter) *Pool {
	return client.NewPoolWithRegistry(1, serviceName, reg)
}

func NewPool(size int, addr string) *Pool {
	return client.NewPool(size, addr)
}

func NewPoolForService(size int, serviceName, addr string) *Pool {
	return client.NewPoolForService(size, serviceName, addr)
}

func NewPoolTLS(size int, addr string, tlsConfig *tls.Config) *Pool {
	return client.NewPoolTLS(size, addr, tlsConfig)
}

func NewPoolTLSForService(size int, serviceName, addr string, tlsConfig *tls.Config) *Pool {
	return client.NewPoolTLSForService(size, serviceName, addr, tlsConfig)
}

func NewPoolWithRegistry(size int, serviceName string, reg registry.RegistryCenter) *Pool {
	return client.NewPoolWithRegistry(size, serviceName, reg)
}
