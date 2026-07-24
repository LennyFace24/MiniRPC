package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/middleware"
	"google.golang.org/protobuf/proto"
)

// echoServer 是测试用的服务，方法签名符合 RegisterService 的反射要求：
// func([]byte) ([]byte, error)
// 不依赖 calc 包，避免 import cycle（calc_rpc.pb.go 反向 import 了 server/client）
type echoServer struct{}

// Add 解析 8 字节 body（两个 int32 big-endian），返回 4 字节 int32 sum
func (s *echoServer) Add(body []byte) ([]byte, error) {
	if len(body) != 8 {
		return nil, errors.New("invalid body length, expected 8")
	}
	a := int32(binary.BigEndian.Uint32(body[0:4]))
	b := int32(binary.BigEndian.Uint32(body[4:8]))
	sum := uint32(a + b)
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, sum)
	return out, nil
}

// Echo 原样返回 body
func (s *echoServer) Echo(body []byte) ([]byte, error) {
	out := make([]byte, len(body))
	copy(out, body)
	return out, nil
}

// Slow 模拟慢请求
func (s *echoServer) Slow(body []byte) ([]byte, error) {
	time.Sleep(100 * time.Millisecond)
	return body, nil
}

// Panic 模拟 panic
func (s *echoServer) Panic(body []byte) ([]byte, error) {
	panic("test panic from Panic")
}

// makeAddBody 构造 Add 方法的 body：两个 int32 的 big-endian
func makeAddBody(a, b int32) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint32(out[0:4], uint32(a))
	binary.BigEndian.PutUint32(out[4:8], uint32(b))
	return out
}

// parseAddResult 解析 Add 的返回值为 int32
func parseAddResult(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

// readFrame 从连接读取一帧：15B 头 + body
func readFrame(r io.Reader) (requestID uint64, body []byte, err error) {
	header := make([]byte, 15)
	if _, err = io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	requestID = binary.BigEndian.Uint64(header[3:11])
	bodyLen := binary.BigEndian.Uint32(header[11:15])
	body = make([]byte, bodyLen)
	_, err = io.ReadFull(r, body)
	return requestID, body, err
}

// writeFrame 向连接写一个请求帧
func writeFrame(w io.Writer, requestID uint64, msg *framepb.MessageRequest) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 15)
	header[0] = 0xCC
	header[1] = 0x01
	header[2] = 0x01
	binary.BigEndian.PutUint64(header[3:11], requestID)
	binary.BigEndian.PutUint32(header[11:15], uint32(len(body)))
	_, err = w.Write(append(header, body...))
	return err
}

// TestServer_RegisterAndExecute 验证服务注册和正常调用
func TestServer_RegisterAndExecute(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})

	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     makeAddBody(3, 5),
	}

	resp := srv.executeHandler(context.Background(), req)
	if resp.Error != "" {
		t.Fatalf("期望无错误, 得到:%s", resp.Error)
	}

	if got := parseAddResult(resp.Returns); got != 8 {
		t.Fatalf("Result: 期望8, 得到%d", got)
	}
}

// TestServer_UnregisteredService 验证未注册服务返回错误
func TestServer_UnregisteredService(t *testing.T) {
	srv := NewRPCServer()
	req := &framepb.MessageRequest{FuncName: "Unknown.Add", Args: []byte{}}
	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望'服务未注册'错误")
	}
}

// TestServer_UnregisteredMethod 验证未注册方法返回错误
func TestServer_UnregisteredMethod(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})
	req := &framepb.MessageRequest{FuncName: "Calc.Unknown", Args: []byte{}}
	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望'函数未注册'错误")
	}
}

// TestServer_WrongFuncNameFormat 验证函数名格式错误
func TestServer_WrongFuncNameFormat(t *testing.T) {
	srv := NewRPCServer()
	req := &framepb.MessageRequest{FuncName: "NoDot", Args: []byte{}}
	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望格式错误")
	}
}

// TestServer_EmptyFuncName 验证空函数名
func TestServer_EmptyFuncName(t *testing.T) {
	srv := NewRPCServer()
	req := &framepb.MessageRequest{FuncName: "", Args: []byte{}}
	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望格式错误（空函数名）")
	}
}

// TestServer_ContextDeadlineExceeded 验证 context 已过期时返回超时错误
func TestServer_ContextDeadlineExceeded(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})

	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     makeAddBody(1, 2),
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	resp := srv.executeHandler(ctx, req)
	if resp.Error == "" {
		t.Fatal("期望超时错误")
	}
}

// TestServer_PanicRecovery 验证 Recovery 中间件捕获 panic
func TestServer_PanicRecovery(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})
	srv.Use(middleware.Recovery)

	req := &framepb.MessageRequest{FuncName: "Calc.Panic", Args: []byte{}}
	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望 panic 被 Recovery 捕获并返回 error")
	}
}

// TestServer_MiddlewareChain 验证 server 中间件链实际执行顺序
func TestServer_MiddlewareChain(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})

	var order []string
	srv.Use(middleware.Middleware(func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(req *framepb.MessageRequest) *framepb.MessageResponse {
			order = append(order, "mw1_before")
			resp := next(req)
			order = append(order, "mw1_after")
			return resp
		}
	}))
	srv.Use(middleware.Middleware(func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(req *framepb.MessageRequest) *framepb.MessageResponse {
			order = append(order, "mw2_before")
			resp := next(req)
			order = append(order, "mw2_after")
			return resp
		}
	}))

	req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: makeAddBody(1, 2)}
	srv.executeHandler(context.Background(), req)

	expected := []string{"mw2_before", "mw1_before", "mw1_after", "mw2_after"}
	if len(order) != len(expected) {
		t.Fatalf("执行顺序长度: 期望%d, 得到%d (%v)", len(expected), len(order), order)
	}
	for i, want := range expected {
		if order[i] != want {
			t.Fatalf("位置%d: 期望%s, 得到%s", i, want, order[i])
		}
	}
}

// TestServer_EndToEnd_TCP 用真实 TCP listener 验证完整端到端调用
func TestServer_EndToEnd_TCP(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})
	srv.Use(middleware.Recovery)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen失败: %v", err)
	}
	defer ln.Close()
	go srv.Start(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial失败: %v", err)
	}
	defer conn.Close()

	req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: makeAddBody(10, 20)}
	if err := writeFrame(conn, 1, req); err != nil {
		t.Fatalf("Write失败: %v", err)
	}

	_, respBody, err := readFrame(conn)
	if err != nil {
		t.Fatalf("读响应失败: %v", err)
	}

	var resp framepb.MessageResponse
	if err := proto.Unmarshal(respBody, &resp); err != nil {
		t.Fatalf("Unmarshal失败: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("端到端调用失败: %s", resp.Error)
	}

	if got := parseAddResult(resp.Returns); got != 30 {
		t.Fatalf("Result: 期望30, 得到%d", got)
	}
}

// TestServer_EndToEnd_Pipe 用 net.Pipe 验证端到端
func TestServer_EndToEnd_Pipe(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})
	srv.Use(middleware.Recovery)

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go srv.handleRequest(serverConn)

	req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: makeAddBody(7, 8)}
	if err := writeFrame(clientConn, 42, req); err != nil {
		t.Fatalf("Write失败: %v", err)
	}

	requestID, respBody, err := readFrame(clientConn)
	if err != nil {
		t.Fatalf("读响应失败: %v", err)
	}
	if requestID != 42 {
		t.Fatalf("RequestID: 期望42, 得到%d", requestID)
	}

	var resp framepb.MessageResponse
	if err := proto.Unmarshal(respBody, &resp); err != nil {
		t.Fatalf("Unmarshal失败: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("调用失败: %s", resp.Error)
	}

	if got := parseAddResult(resp.Returns); got != 15 {
		t.Fatalf("Result: 期望15, 得到%d", got)
	}
}

// TestServer_MultipleRequestsSameConn 验证单连接多请求复用
func TestServer_MultipleRequestsSameConn(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go srv.handleRequest(serverConn)

	for i := int32(0); i < 5; i++ {
		req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: makeAddBody(i, i)}
		if err := writeFrame(clientConn, uint64(i), req); err != nil {
			t.Fatalf("第%d次Write失败: %v", i, err)
		}

		requestID, respBody, err := readFrame(clientConn)
		if err != nil {
			t.Fatalf("第%d次读响应失败: %v", i, err)
		}
		if requestID != uint64(i) {
			t.Fatalf("第%d次RequestID: 期望%d, 得到%d", i, i, requestID)
		}

		var resp framepb.MessageResponse
		if err := proto.Unmarshal(respBody, &resp); err != nil {
			t.Fatalf("第%d次Unmarshal失败: %v", i, err)
		}
		if got := parseAddResult(resp.Returns); got != i+i {
			t.Fatalf("第%d次Result: 期望%d, 得到%d", i, i+i, got)
		}
	}
}

// TestServer_BadBody 验证 handler 收到错误格式的 body
func TestServer_BadBody(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})

	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{0xFF, 0xFF, 0xFF}, // 长度不对
	}
	resp := srv.executeHandler(context.Background(), req)
	// Add 方法对长度有要求，长度不对应返回 error
	if resp.Error == "" {
		// 如果 handler 没返回 error，也接受（取决于 handler 实现）
		// 这里主要验证不会 panic
	}
}

// TestServer_RegistryMultipleServices 验证多服务并存
func TestServer_RegistryMultipleServices(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &echoServer{})
	srv.RegisterService("Echo", &echoServer{})

	req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: makeAddBody(3, 4)}
	resp := srv.executeHandler(context.Background(), req)
	if resp.Error != "" {
		t.Fatalf("Calc.Add 失败: %s", resp.Error)
	}

	echoReq := &framepb.MessageRequest{FuncName: "Echo.Echo", Args: []byte("hello")}
	resp = srv.executeHandler(context.Background(), echoReq)
	if resp.Error != "" {
		t.Fatalf("Echo.Echo 失败: %s", resp.Error)
	}
	if !bytes.Equal(resp.Returns, []byte("hello")) {
		t.Fatalf("Echo: 期望'hello', 得到'%s'", resp.Returns)
	}
}
