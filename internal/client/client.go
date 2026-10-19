package client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/config"
	"mini-rpc/internal/protocol"
	"mini-rpc/internal/transport"

	"google.golang.org/protobuf/proto"
)

// pendingCall 表示一个在途请求。
// gen 记录请求注册时所属的连接代际，断线/重连时只清理同一代际的请求，
// 避免旧 readLoop 误伤新连接上的 pending。
type pendingCall struct {
	ch  chan *RPCResult
	gen uint64
}

// RPCClient 是一个长连接的 RPC 客户端，支持多路复用和自动重连。
type RPCClient struct {
	addr      string // 显式地址；为空时回退到 config.yaml
	tlsConfig *tls.Config

	mu       sync.Mutex
	conn     net.Conn
	gen      uint64 // 连接代际，每次 dial 成功递增
	closed   bool
	pending  map[uint64]*pendingCall
	nextID   uint64
	inflight atomic.Int64 // 在途请求数，供 LeastConnections 使用

	writeMu sync.Mutex // 串行化 conn.Write，避免并发写导致帧交错
}

type RPCResult struct {
	Body  []byte
	Error error
}

// Future 表示一个异步调用结果。
// Get 会阻塞直到响应到达或 ctx 超时。
type Future struct {
	ch     chan *RPCResult
	cancel func() // ctx 超时/取消时调用，清理 pending
}

// Get 阻塞等待结果；ctx 超时会返回 ctx.Err() 并清理 pending entry。
func (f *Future) Get(ctx context.Context) ([]byte, error) {
	defer f.cancel()
	select {
	case r := <-f.ch:
		return r.Body, r.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func NewRPCClient() *RPCClient {
	return &RPCClient{
		nextID:  1,
		pending: make(map[uint64]*pendingCall),
	}
}

// NewRPCClientWithAddr 创建一个指定服务器地址的客户端，不依赖 config.yaml。
func NewRPCClientWithAddr(addr string) *RPCClient {
	c := NewRPCClient()
	c.addr = addr
	return c
}

// Close 关闭客户端，所有 pending 调用立即失败。
func (c *RPCClient) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	c.failAllPending(errors.New("[client.go]client 已关闭"))

	if conn != nil {
		return conn.Close()
	}
	return nil
}

// dial 建立新连接并启动 readLoop。调用方必须持锁或确保不被并发调用。
func (c *RPCClient) dial() error {
	var address string
	if c.addr != "" {
		address = c.addr
	} else {
		cfg := config.LoadConfig()
		if cfg == nil {
			return errors.New("[client.go]加载配置文件失败")
		}
		address = net.JoinHostPort(cfg.Server.URL, strconv.Itoa(cfg.Server.Port))
	}
	var conn net.Conn
	var err error
	if c.tlsConfig != nil {
		conn, err = tls.Dial("tcp", address, c.tlsConfig)
	} else {
		conn, err = net.Dial("tcp", address)
	}
	if err != nil {
		return fmt.Errorf("[client.go]连接服务器失败:%w", err)
	}
	c.gen++
	gen := c.gen
	c.conn = conn
	go c.readLoop(conn, gen)
	return nil
}

// ensureConn 保证连接可用，断开则自动重连。替代原来的 sync.Once。
func (c *RPCClient) ensureConn() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("[client.go]client 已关闭")
	}
	if c.conn != nil {
		return nil
	}
	return c.dial()
}

// triggerReconnect 关闭旧连接并把 conn 置空，下次 CallAsync 会重新 dial。
func (c *RPCClient) triggerReconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// CallAsync 发起一次异步调用。返回 Future 用于获取结果。
// 如果发送失败会立即返回 error（不返回 nil Future 让调用者盲等）。
func (c *RPCClient) CallAsync(ctx context.Context, funcName string, body []byte) (*Future, error) {
	if err := c.ensureConn(); err != nil {
		return nil, err
	}

	// 从 ctx 提取 deadline 写入 metadata，让 server 端也能感知超时
	metadata := make(map[string]string)
	if deadline, ok := ctx.Deadline(); ok {
		transport.SetDeadlineMetadata(metadata, deadline)
	}

	c.mu.Lock()
	if c.closed || c.conn == nil {
		c.mu.Unlock()
		return nil, errors.New("[client.go]client 已关闭")
	}
	id := c.nextID
	c.nextID++
	gen := c.gen
	conn := c.conn
	ch := make(chan *RPCResult, 1)
	c.pending[id] = &pendingCall{ch: ch, gen: gen}
	c.inflight.Add(1)
	c.mu.Unlock()

	// 清理函数：超时/取消时把 pending entry 删掉，避免 readLoop 后续向 nil channel 发送
	cancel := func() {
		c.mu.Lock()
		if _, ok := c.pending[id]; ok {
			delete(c.pending, id)
			c.inflight.Add(-1)
		}
		c.mu.Unlock()
	}

	req := &framepb.MessageRequest{
		FuncName: funcName,
		Args:     body,
		Metadata: metadata,
	}

	// 串行化 Write，避免多 goroutine 并发 Write 导致 TCP 帧交错
	c.writeMu.Lock()
	err := transport.SendToServer(id, conn, req)
	c.writeMu.Unlock()
	if err != nil {
		// 发送失败：立即清理 pending，并触发重连
		cancel()
		c.triggerReconnect()
		return nil, fmt.Errorf("[client.go]发送请求错误:%w", err)
	}

	return &Future{ch: ch, cancel: cancel}, nil
}

// readLoop 持续读取响应并分发给对应的 pending channel。
// 任何错误都会 fail-fast 唤醒本代际的等待者，然后清理自己负责的 conn。
// 注意：只操作启动时捕获的 conn，避免重连后误读/误关新连接。
func (c *RPCClient) readLoop(conn net.Conn, gen uint64) {
	for {
		requestID, resp, err := c.readOneResponse(conn)
		if err != nil {
			log.Printf("[client.go]readLoop 退出:%v", err)
			failErr := fmt.Errorf("[client.go]连接断开:%w", err)
			c.failPendingGen(gen, failErr)
			// 只在 c.conn 还是我这个旧 conn 时才清理；
			// 如果已经被新连接替换，说明重连已发生，不要动
			c.mu.Lock()
			if c.conn == conn {
				conn.Close()
				c.conn = nil
			}
			c.mu.Unlock()
			return
		}

		c.mu.Lock()
		pc, ok := c.pending[requestID]
		if ok {
			// 消费即删：避免后续向 nil channel 发送
			delete(c.pending, requestID)
			c.inflight.Add(-1)
		}
		c.mu.Unlock()

		if !ok {
			// 调用方已经超时清理了 entry，迟到响应直接丢弃
			continue
		}

		result := &RPCResult{Body: resp.Returns}
		if resp.Error != "" {
			result.Error = errors.New(resp.Error)
		}
		select {
		case pc.ch <- result:
		default:
			// channel 已被消费（超时），丢弃
		}
	}
}

// readOneResponse 读取一帧响应。
func (c *RPCClient) readOneResponse(conn net.Conn) (uint64, *framepb.MessageResponse, error) {
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, fmt.Errorf("[client.go]读响应头错误:%w", err)
	}
	if _, err := protocol.CheckProtocolHeader(header); err != nil {
		return 0, nil, fmt.Errorf("[client.go]响应协议头校验错误:%w", err)
	}
	bodyLen := binary.BigEndian.Uint32(header[11:15])
	if bodyLen > protocol.MaxFrameSize {
		return 0, nil, fmt.Errorf("[client.go]响应体过大:%d", bodyLen)
	}
	msg := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, msg); err != nil {
		return 0, nil, fmt.Errorf("[client.go]读响应体错误:%w", err)
	}

	requestID := binary.BigEndian.Uint64(header[3:11])
	var resp framepb.MessageResponse
	if err := proto.Unmarshal(msg, &resp); err != nil {
		return 0, nil, fmt.Errorf("[client.go]反序列化响应错误:%w", err)
	}
	return requestID, &resp, nil
}

// failPendingGen 唤醒指定连接代际的等待者，避免连接断开时 future 永久阻塞。
// 其他代际的 pending 属于新连接，不能在这里清理。
func (c *RPCClient) failPendingGen(gen uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, pc := range c.pending {
		if pc.gen != gen {
			continue
		}
		select {
		case pc.ch <- &RPCResult{Error: err}:
		default:
		}
		delete(c.pending, id)
		c.inflight.Add(-1)
	}
}

// failAllPending 在 Close 时唤醒所有等待者。
func (c *RPCClient) failAllPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, pc := range c.pending {
		select {
		case pc.ch <- &RPCResult{Error: err}:
		default:
		}
		delete(c.pending, id)
		c.inflight.Add(-1)
	}
}
