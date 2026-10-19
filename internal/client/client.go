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
	"sync"
	"sync/atomic"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/protocol"
	"mini-rpc/internal/transport"

	"google.golang.org/protobuf/proto"
)

const (
	defaultDialTimeout  = 3 * time.Second
	defaultWriteTimeout = 10 * time.Second
	defaultMaxInflight  = 1024
)

var errClientClosed = errors.New("[client.go]client 已关闭")

// pendingCall 表示一个在途请求。
// gen 记录请求注册时所属的连接代际，断线/重连时只清理同一代际的请求，
// 避免旧 readLoop 误伤新连接上的 pending。
type pendingCall struct {
	ch  chan *RPCResult
	gen uint64
}

// RPCClient 是一个长连接的 RPC 客户端，支持多路复用和自动重连。
type RPCClient struct {
	addr         string // 显式地址；为空时 CallAsync 返回错误
	tlsConfig    *tls.Config
	dialTimeout  time.Duration
	writeTimeout time.Duration
	slots        chan struct{} // 应用层背压：限制单连接最大在途请求数

	mu         sync.Mutex
	conn       net.Conn
	gen        uint64 // 连接代际，每次 dial 成功递增
	closed     bool
	dialing    chan struct{} // 非 nil 表示有 goroutine 正在 dial
	dialCancel context.CancelFunc
	pending    map[uint64]*pendingCall
	nextID     uint64
	inflight   atomic.Int64 // 在途请求数，供 LeastConnections 使用

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

// NewRPCClient 创建一个未配置地址的客户端。
// 正常发起调用请使用 NewRPCClientWithAddr，或通过 Pool 注入地址。
func NewRPCClient() *RPCClient {
	return &RPCClient{
		nextID:       1,
		pending:      make(map[uint64]*pendingCall),
		dialTimeout:  defaultDialTimeout,
		writeTimeout: defaultWriteTimeout,
		slots:        make(chan struct{}, defaultMaxInflight),
	}
}

// NewRPCClientWithAddr 创建一个指定服务器地址的客户端。
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
	cancel := c.dialCancel
	c.dialCancel = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	c.failAllPending(errClientClosed)

	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *RPCClient) address() (string, error) {
	if c.addr == "" {
		return "", errors.New("[client.go]未配置服务器地址")
	}
	return c.addr, nil
}

// dial 建立新连接并启动 readLoop。调用方不能持有 c.mu，且同一时刻只能有一个 dial。
func (c *RPCClient) dial(ctx context.Context) error {
	address, err := c.address()
	if err != nil {
		return err
	}

	dialer := &net.Dialer{Timeout: c.dialTimeout}
	var conn net.Conn
	if c.tlsConfig != nil {
		tlsDialer := &tls.Dialer{
			NetDialer: dialer,
			Config:    c.tlsConfig,
		}
		conn, err = tlsDialer.DialContext(ctx, "tcp", address)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return fmt.Errorf("[client.go]连接服务器失败:%w", err)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		return errClientClosed
	}
	if c.conn != nil {
		c.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	c.gen++
	gen := c.gen
	c.conn = conn
	c.mu.Unlock()

	go c.readLoop(conn, gen)
	return nil
}

// ensureConn 保证连接可用。只有第一个调用方负责 dial，其他调用方等待结果。
func (c *RPCClient) ensureConn(ctx context.Context) error {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return errClientClosed
		}
		if c.conn != nil {
			c.mu.Unlock()
			return nil
		}
		if c.dialing != nil {
			ch := c.dialing
			c.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		ch := make(chan struct{})
		c.dialing = ch
		dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
		c.dialCancel = cancel
		c.mu.Unlock()

		err := c.dial(dialCtx)

		c.mu.Lock()
		c.dialing = nil
		c.dialCancel = nil
		close(ch)
		c.mu.Unlock()
		cancel()

		if err != nil {
			return err
		}
		// 回到循环确认连接仍然有效；Close 可能在 dial 期间发生。
	}
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

func (c *RPCClient) acquireSlot(ctx context.Context) error {
	if c.slots == nil {
		return nil
	}
	select {
	case c.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *RPCClient) releaseSlot() {
	if c.slots == nil {
		return
	}
	select {
	case <-c.slots:
	default:
	}
}

// CallAsync 发起一次异步调用。返回 Future 用于获取结果。
// 如果发送失败会立即返回 error（不返回 nil Future 让调用者盲等）。
func (c *RPCClient) CallAsync(ctx context.Context, funcName string, body []byte) (*Future, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.ensureConn(ctx); err != nil {
		return nil, err
	}
	if err := c.acquireSlot(ctx); err != nil {
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
		c.releaseSlot()
		return nil, errClientClosed
	}
	id := c.nextID
	c.nextID++
	gen := c.gen
	conn := c.conn
	ch := make(chan *RPCResult, 1)
	c.pending[id] = &pendingCall{ch: ch, gen: gen}
	c.inflight.Add(1)
	c.mu.Unlock()

	// 清理函数：超时/取消时把 pending entry 删掉，并释放背压槽位。
	cancel := func() {
		c.mu.Lock()
		if _, ok := c.pending[id]; ok {
			delete(c.pending, id)
			c.inflight.Add(-1)
			c.releaseSlot()
		}
		c.mu.Unlock()
	}

	req := &framepb.MessageRequest{
		FuncName: funcName,
		Args:     body,
		Metadata: metadata,
	}

	// 串行化 Write，避免多 goroutine 并发 Write 导致 TCP 帧交错。
	c.writeMu.Lock()
	if c.writeTimeout > 0 {
		deadline := time.Now().Add(c.writeTimeout)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		_ = conn.SetWriteDeadline(deadline)
	}
	err := transport.SendToServer(id, conn, req)
	if c.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Time{})
	}
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
			c.mu.Lock()
			expectedClose := c.closed
			c.mu.Unlock()
			if !expectedClose {
				log.Printf("[client.go]readLoop 退出:%v", err)
			}
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
			c.releaseSlot()
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
		c.releaseSlot()
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
		c.releaseSlot()
	}
}
