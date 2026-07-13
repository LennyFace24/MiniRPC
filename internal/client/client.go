package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"mini-rpc/internal/codec"
	"mini-rpc/internal/config"
	"mini-rpc/internal/transport"
	"mini-rpc/internal/types"
)

type RPCClient struct {
	conn    net.Conn
	pending map[uint64]chan *RPCResult
	nextID  uint64
	mu      sync.Mutex
	once    sync.Once
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
		once:    sync.Once{},
	}
}

func (c *RPCClient) CallAsync(funcName string, args ...interface{}) *Future {
	// 创建唯一的连接
	c.once.Do(func() {
		// 获取服务器地址
		config := config.LoadConfig()
		if config == nil {
			log.Fatalf("[client.go]加载配置文件失败")
		}
		// port是int类型，需要转换为string
		address := fmt.Sprintf("%s:%d", config.Server.URL, config.Server.Port)
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Fatalf("[client.go]连接服务器失败:%v", err)
		}
		c.conn = conn

		go c.readLoop() // 启动读取响应的协程
	})

	// 读写锁
	c.mu.Lock()

	id := c.nextID
	c.nextID++
	ch := make(chan *RPCResult, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	// 发送请求
	c.Call(id, funcName, args...)

	return &Future{ch: ch}
}

func (c *RPCClient) Call(requestId uint64, funcName string, args ...interface{}) {

	// 封装请求体
	request := types.RequestData{
		FuncName:  funcName,
		Arguments: args,
	}
	// 发送请求
	// sendserver 只负责写
	err := transport.SendToServer(requestId, c.conn, request)
	if err != nil {
		log.Printf("[client.go]发送请求错误:%v", err)
		return
	}
}

func (c *RPCClient) readLoop() {
	for {
		header := make([]byte, 15)
		io.ReadFull(c.conn, header)
		bodyLen := binary.BigEndian.Uint32(header[11:15])
		totalLen := 15 + bodyLen
		msg := make([]byte, totalLen) // 接收到的响应
		copy(msg, header)
		io.ReadFull(c.conn, msg[15:])

		// 获取请求id
		requestID := binary.BigEndian.Uint64(header[3:11])

		// 反序列化
		var response types.ResponseData
		codec.Deserialize(msg[15:], &response)

		// 存入channel
		if response.Error != "" {
			log.Printf("[client.go]读取响应出错:" + response.Error)
			return
		}
		c.pending[requestID] <- &RPCResult{Returns: response.Returns, Error: nil} // 这里需要根据实际情况填充返回值
	}
}
