package client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"google.golang.org/protobuf/proto"
	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/config"
	"mini-rpc/internal/transport"
)

type RPCClient struct {
	conn      net.Conn
	pending   map[uint64]chan *RPCResult
	nextID    uint64
	mu        sync.Mutex
	once      sync.Once
	tlsConfig *tls.Config
}

type RPCResult struct {
	Body  []byte
	Error error
}

type Future struct {
	ch chan *RPCResult
}

func (f *Future) Get() ([]byte, error) {
	result := <-f.ch
	return result.Body, result.Error
}

func NewRPCClient() *RPCClient {
	return &RPCClient{
		nextID:  1,
		pending: make(map[uint64]chan *RPCResult),
		mu:      sync.Mutex{},
		once:    sync.Once{},
	}
}

func (c *RPCClient) CallAsync(ctx context.Context, funcName string, body []byte) *Future {

	deadline, _ := ctx.Deadline()
	metadata := make(map[string]string)
	if !deadline.IsZero() {
		metadata["deadline"] = fmt.Sprintf("%d", deadline.UnixNano())
	}

	c.once.Do(func() {
		config := config.LoadConfig()
		if config == nil {
			log.Fatalf("[client.go]加载配置文件失败")
		}
		address := fmt.Sprintf("%s:%d", config.Server.URL, config.Server.Port)
		var conn net.Conn
		var err error
		if c.tlsConfig != nil {
			conn, err = tls.Dial("tcp", address, c.tlsConfig)
		} else {
			conn, err = net.Dial("tcp", address)
		}
		if err != nil {
			log.Fatalf("[client.go]连接服务器失败:%v", err)
		}
		c.conn = conn
		go c.readLoop()
	})

	c.mu.Lock()
	id := c.nextID
	c.nextID++
	ch := make(chan *RPCResult, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	req := &framepb.MessageRequest{
		FuncName: funcName,
		Args:     body,
		Metadata: metadata,
	}
	err := transport.SendToServer(id, c.conn, req)
	if err != nil {
		log.Printf("[client.go]发送请求错误:%v", err)
	}

	return &Future{ch: ch}
}

func (c *RPCClient) readLoop() {
	for {
		header := make([]byte, 15)
		_, err := io.ReadFull(c.conn, header)
		if err != nil {
			log.Printf("[client.go]读响应头错误:%v", err)
			return
		}
		bodyLen := binary.BigEndian.Uint32(header[11:15])
		totalLen := 15 + bodyLen
		msg := make([]byte, totalLen)
		copy(msg, header)
		_, err = io.ReadFull(c.conn, msg[15:])
		if err != nil {
			log.Printf("[client.go]读响应体错误:%v", err)
			return
		}

		requestID := binary.BigEndian.Uint64(header[3:11])

		var response framepb.MessageResponse
		err = proto.Unmarshal(msg[15:], &response)
		if err != nil {
			log.Printf("[client.go]反序列化响应错误:%v", err)
			return
		}

		if response.Error != "" {
			log.Printf("[client.go]读取响应出错: %s", response.Error)
			return
		}
		c.pending[requestID] <- &RPCResult{Body: response.Returns, Error: nil}
	}
}