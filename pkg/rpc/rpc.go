package rpc

import (
	"mini-rpc/internal/client"
	"mini-rpc/internal/server"
)

func NewServer() *server.RPCServer {
	return server.NewRPCServer()
}

func NewClient() *client.RPCClient {
	return client.NewRPCClient()
}
