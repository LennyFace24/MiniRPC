package client

import (
	"fmt"
	"log"
	"net"
	"sync"

	"mini-rpc/internal/codec"
	"mini-rpc/internal/transport"
	"mini-rpc/internal/types"
)

type RPCClient struct {
	conn    net.Conn
	pending map[uint64]chan *RPCResult
	nextID  uint64
	mu      sync.Mutex
}

type RPCResult struct {
	Returns []interface{}
	Error   error
}

type Future struct {
	ch chan *RPCResult
}

// 异步获取
func (f *Future) Get() ([]interface{}, error) {
	result := <-f.ch
	return result.Returns, result.Error
}

func NewRPCClient() *RPCClient {
	return &RPCClient{
		nextID:  1,
		pending: make(map[uint64]chan *RPCResult),
		mu:      sync.Mutex{},
	}
}

func (c *RPCClient) CallAsync(funcName string, args ...interface{}) *Future {
	// 读写锁
	c.mu.Lock()

	id := c.nextID
	c.nextID++
	ch := make(chan *RPCResult, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	go func() {
		result, err := c.Call(id, funcName, args...)
		ch <- &RPCResult{Returns: result, Error: err}
	}()

	return &Future{ch: ch}
}

func (c *RPCClient) Call(requestId uint64, funcName string, args ...interface{}) ([]interface{}, error) {

	// 封装请求体
	request := types.RequestData{
		FuncName:  funcName,
		Arguments: args,
	}
	// 发送请求
	resp, err := transport.SendToServer(requestId, request)
	if err != nil {
		log.Printf("[client.go]发送请求错误:%v", err)
		return nil, err
	}
	// 读取响应
	var response types.ResponseData
	// 反序列化
	err = codec.Deserialize(resp, &response)
	if err != nil {
		log.Printf("[client.go]反序列化响应体错误:%v", err)
		return nil, err
	}
	if response.Error != "" {
		log.Printf("[client.go]服务器返回错误:%v", response.Error)
		return nil, fmt.Errorf("[client.go]服务器返回错误:%v", response.Error)
	}
	return response.Returns, nil
}
