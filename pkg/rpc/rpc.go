package rpc

import (
	"mini-rpc/internal/client"
	"mini-rpc/internal/server"
	"mini-rpc/pkg/registry"
)

func NewServer() *server.RPCServer {
	return server.NewRPCServer()
}

func NewClient() *client.RPCClient {
	return client.NewRPCClient()
}

func NewPool(size int, addr string) *client.Pool {
	return client.NewPool(size, addr)
}

func NewPoolForService(size int, serviceName, addr string) *client.Pool {
	return client.NewPoolForService(size, serviceName, addr)
}

func NewPoolWithRegistry(size int, serviceName string, registry registry.RegistryCenter) *client.Pool {
	return client.NewPoolWithRegistry(size, serviceName, registry)
}
