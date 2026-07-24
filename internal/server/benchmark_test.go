package server

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/middleware"
	"google.golang.org/protobuf/proto"
)

// benchHandler 用于 benchmark 的服务实现（不依赖 calc，避免 import cycle）
type benchHandler struct{}

func (s *benchHandler) Add(body []byte) ([]byte, error) {
	if len(body) != 8 {
		return nil, nil
	}
	a := int32(binary.BigEndian.Uint32(body[0:4]))
	b := int32(binary.BigEndian.Uint32(body[4:8]))
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(a+b))
	return out, nil
}

// startBenchServer 启动一个真实 TCP server 用于 benchmark
func startBenchServer(b *testing.B) (string, func()) {
	b.Helper()
	srv := NewRPCServer()
	srv.RegisterService("Calc", &benchHandler{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Listen失败: %v", err)
	}
	go srv.Start(ln)
	return ln.Addr().String(), func() { ln.Close() }
}

// makeBenchPacket 构造一个完整的请求包（15B头 + body）
func makeBenchPacket(requestID uint64, a, b int32) []byte {
	args := make([]byte, 8)
	binary.BigEndian.PutUint32(args[0:4], uint32(a))
	binary.BigEndian.PutUint32(args[4:8], uint32(b))

	req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: args}
	body, _ := proto.Marshal(req)

	header := make([]byte, 15)
	header[0] = 0xCC
	header[1] = 0x01
	header[2] = 0x01
	binary.BigEndian.PutUint64(header[3:11], requestID)
	binary.BigEndian.PutUint32(header[11:15], uint32(len(body)))
	return append(header, body...)
}

// readBenchResponse 从连接读取一个响应帧
func readBenchResponse(r io.Reader) (*framepb.MessageResponse, error) {
	header := make([]byte, 15)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	bodyLen := binary.BigEndian.Uint32(header[11:15])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	var resp framepb.MessageResponse
	if err := proto.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// BenchmarkServer_TCP_Serial 真实 TCP 单连接串行调用性能
func BenchmarkServer_TCP_Serial(b *testing.B) {
	b.ReportAllocs()
	addr, stop := startBenchServer(b)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("Dial失败: %v", err)
	}
	defer conn.Close()

	packet := makeBenchPacket(1, 3, 5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(packet); err != nil {
			b.Fatalf("Write失败: %v", err)
		}
		resp, err := readBenchResponse(conn)
		if err != nil {
			b.Fatalf("读响应失败: %v", err)
		}
		if resp.Error != "" {
			b.Fatalf("调用失败: %s", resp.Error)
		}
	}
}

// BenchmarkServer_TCP_Parallel 真实 TCP 多连接并发调用性能
func BenchmarkServer_TCP_Parallel(b *testing.B) {
	b.ReportAllocs()
	addr, stop := startBenchServer(b)
	defer stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatalf("Dial失败: %v", err)
		}
		defer conn.Close()

		packet := makeBenchPacket(1, 3, 5)
		for pb.Next() {
			if _, err := conn.Write(packet); err != nil {
				b.Fatalf("Write失败: %v", err)
			}
			if _, err := readBenchResponse(conn); err != nil {
				b.Fatalf("读响应失败: %v", err)
			}
		}
	})
}

// BenchmarkServer_Pipe_Serial net.Pipe 单连接串行（纯进程内开销基线）
func BenchmarkServer_Pipe_Serial(b *testing.B) {
	b.ReportAllocs()
	srv := NewRPCServer()
	srv.RegisterService("Calc", &benchHandler{})

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go srv.handleRequest(serverConn)

	packet := makeBenchPacket(1, 3, 5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := clientConn.Write(packet); err != nil {
			b.Fatalf("Write失败: %v", err)
		}
		if _, err := readBenchResponse(clientConn); err != nil {
			b.Fatalf("读响应失败: %v", err)
		}
	}
}

// BenchmarkServer_ExecuteHandler 纯 handler 调用（无 IO，无协议解析）
func BenchmarkServer_ExecuteHandler(b *testing.B) {
	b.ReportAllocs()
	srv := NewRPCServer()
	srv.RegisterService("Calc", &benchHandler{})

	args := make([]byte, 8)
	binary.BigEndian.PutUint32(args[0:4], 3)
	binary.BigEndian.PutUint32(args[4:8], 5)
	req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: args}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := srv.executeHandler(ctx, req)
		if resp.Error != "" {
			b.Fatalf("调用失败: %s", resp.Error)
		}
	}
}

// BenchmarkServer_WithMiddleware 带 3 个中间件的 handler 调用性能
func BenchmarkServer_WithMiddleware(b *testing.B) {
	b.ReportAllocs()
	srv := NewRPCServer()
	srv.RegisterService("Calc", &benchHandler{})

	noopMW := middleware.Middleware(func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(req *framepb.MessageRequest) *framepb.MessageResponse {
			return next(req)
		}
	})
	srv.Use(noopMW)
	srv.Use(noopMW)
	srv.Use(noopMW)

	args := make([]byte, 8)
	binary.BigEndian.PutUint32(args[0:4], 3)
	binary.BigEndian.PutUint32(args[4:8], 5)
	req := &framepb.MessageRequest{FuncName: "Calc.Add", Args: args}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.executeHandler(ctx, req)
	}
}

// BenchmarkProtoMarshal 单纯 proto 序列化开销基线
func BenchmarkProtoMarshal(b *testing.B) {
	b.ReportAllocs()
	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{8, 3, 16, 5},
		Metadata: map[string]string{"trace-id": "abc"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proto.Marshal(req)
	}
}

// BenchmarkProtoUnmarshal 单纯 proto 反序列化开销基线
func BenchmarkProtoUnmarshal(b *testing.B) {
	b.ReportAllocs()
	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{8, 3, 16, 5},
		Metadata: map[string]string{"trace-id": "abc"},
	}
	data, _ := proto.Marshal(req)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var got framepb.MessageRequest
		proto.Unmarshal(data, &got)
	}
}

// BenchmarkServer_ConcurrentThroughput 并发吞吐量测试
func BenchmarkServer_ConcurrentThroughput(b *testing.B) {
	b.ReportAllocs()
	addr, stop := startBenchServer(b)
	defer stop()

	const concurrency = 16
	var wg sync.WaitGroup
	wg.Add(concurrency)

	conns := make([]net.Conn, concurrency)
	for i := range conns {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatalf("Dial失败: %v", err)
		}
		conns[i] = conn
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	packet := makeBenchPacket(1, 3, 5)

	b.ResetTimer()
	perWorker := b.N / concurrency
	if perWorker == 0 {
		perWorker = 1
	}
	for i := 0; i < concurrency; i++ {
		go func(conn net.Conn) {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if _, err := conn.Write(packet); err != nil {
					b.Errorf("Write失败: %v", err)
					return
				}
				if _, err := readBenchResponse(conn); err != nil {
					b.Errorf("读响应失败: %v", err)
					return
				}
			}
		}(conns[i])
	}
	wg.Wait()
}
