package transport

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"google.golang.org/protobuf/proto"
)

// readFrame 低层读 15B 协议头 + body，返回 (requestID, body, error)
// 用于测试中模拟 client 端读取响应帧
func readFrame(r io.Reader) (uint64, []byte, error) {
	header := make([]byte, 15)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	if header[0] != 0xCC || header[1] != 0x01 || header[2] != 0x01 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	requestID := binary.BigEndian.Uint64(header[3:11])
	bodyLen := binary.BigEndian.Uint32(header[11:15])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return requestID, body, nil
}

// TestSendToServer_ReadAndDeserialize_RequestRoundTrip 验证请求方向完整往返
// client 用 SendToServer 写入 → server 用 ReadAndDeserialize 读出
func TestSendToServer_ReadAndDeserialize_RequestRoundTrip(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	req := &framepb.MessageRequest{
		FuncName: "Calculator.Add",
		Args:     []byte{8, 3, 16, 5},
		Metadata: map[string]string{"trace-id": "abc-123", "timeout": "1000"},
	}

	done := make(chan error, 1)
	go func() { done <- SendToServer(42, clientConn, req) }()

	got, gotID, err := ReadAndDeserialize(serverConn)
	if err != nil {
		t.Fatalf("ReadAndDeserialize错误: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("SendToServer错误: %v", err)
	}

	if gotID != 42 {
		t.Fatalf("RequestID: 期望42, 得到%d", gotID)
	}
	if got.FuncName != "Calculator.Add" {
		t.Fatalf("FuncName: 期望Calculator.Add, 得到%s", got.FuncName)
	}
	if !bytes.Equal(got.Args, req.Args) {
		t.Fatalf("Args: 期望%v, 得到%v", req.Args, got.Args)
	}
	if got.Metadata["trace-id"] != "abc-123" {
		t.Fatalf("trace-id丢失: %v", got.Metadata)
	}
	if got.Metadata["timeout"] != "1000" {
		t.Fatalf("timeout丢失: %v", got.Metadata)
	}
}

// TestSendToClient_ResponseBytes 验证 SendToClient 写出的字节能被解析为 MessageResponse
func TestSendToClient_ResponseBytes(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	resp := &framepb.MessageResponse{
		Returns: []byte{8, 8},
		Error:   "",
	}

	done := make(chan error, 1)
	go func() { done <- SendToClient(99, clientConn, resp) }()

	requestID, body, err := readFrame(serverConn)
	if err != nil {
		t.Fatalf("readFrame错误: %v", err)
	}
	if requestID != 99 {
		t.Fatalf("RequestID: 期望99, 得到%d", requestID)
	}

	var gotResp framepb.MessageResponse
	if err := proto.Unmarshal(body, &gotResp); err != nil {
		t.Fatalf("Unmarshal MessageResponse失败: %v", err)
	}
	if !bytes.Equal(gotResp.Returns, resp.Returns) {
		t.Fatalf("Returns: 期望%v, 得到%v", resp.Returns, gotResp.Returns)
	}
	if gotResp.Error != "" {
		t.Fatalf("Error: 期望空, 得到'%s'", gotResp.Error)
	}

	if err := <-done; err != nil {
		t.Fatalf("SendToClient错误: %v", err)
	}
}

// TestSendToClient_WithError 验证带 error 的响应能正确传输
func TestSendToClient_WithError(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	resp := &framepb.MessageResponse{
		Returns: nil,
		Error:   "service unavailable",
	}

	done := make(chan error, 1)
	go func() { done <- SendToClient(1, clientConn, resp) }()

	_, body, err := readFrame(serverConn)
	if err != nil {
		t.Fatalf("readFrame错误: %v", err)
	}
	var gotResp framepb.MessageResponse
	if err := proto.Unmarshal(body, &gotResp); err != nil {
		t.Fatalf("Unmarshal失败: %v", err)
	}
	if gotResp.Error != "service unavailable" {
		t.Fatalf("Error: 期望'service unavailable', 得到'%s'", gotResp.Error)
	}
	<-done
}

// TestReadAndDeserialize_ConnectionClosed 验证连接关闭时返回错误
func TestReadAndDeserialize_ConnectionClosed(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	serverConn.Close()
	clientConn.Close()

	_, _, err := ReadAndDeserialize(serverConn)
	if err == nil {
		t.Fatal("期望连接关闭错误, 但未返回")
	}
}

// TestSendToServer_ClosedConn 验证向已关闭连接发送返回错误
func TestSendToServer_ClosedConn(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	serverConn.Close()
	clientConn.Close()

	err := SendToServer(1, clientConn, &framepb.MessageRequest{FuncName: "x"})
	if err == nil {
		t.Fatal("期望发送错误, 但未返回")
	}
}

// TestSendToClient_ClosedConn 验证向已关闭连接发送响应返回错误
func TestSendToClient_ClosedConn(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	serverConn.Close()
	clientConn.Close()

	err := SendToClient(1, clientConn, &framepb.MessageResponse{})
	if err == nil {
		t.Fatal("期望发送错误, 但未返回")
	}
}

// TestSendToServer_LargeBody 验证大 body 的传输（>64KB）
func TestSendToServer_LargeBody(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	largeArgs := make([]byte, 64*1024)
	for i := range largeArgs {
		largeArgs[i] = byte(i % 256)
	}
	req := &framepb.MessageRequest{
		FuncName: "Big.Upload",
		Args:     largeArgs,
	}

	done := make(chan error, 1)
	go func() { done <- SendToServer(1000, clientConn, req) }()

	got, gotID, err := ReadAndDeserialize(serverConn)
	if err != nil {
		t.Fatalf("ReadAndDeserialize错误: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("SendToServer错误: %v", err)
	}
	if gotID != 1000 {
		t.Fatalf("RequestID: 期望1000, 得到%d", gotID)
	}
	if !bytes.Equal(got.Args, largeArgs) {
		t.Fatalf("大body数据不一致")
	}
}

// TestConcurrentSendAndReceive 验证并发写入场景下协议层无串扰
// net.Pipe 不支持多个 goroutine 并发 Read 各自读到完整帧，所以用一个 reader goroutine 顺序消费
func TestConcurrentSendAndReceive(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	const N = 100

	// 单个 reader goroutine 顺序读 N 个帧（net.Pipe 的 Read 不支持并发）
	readerDone := make(chan error, 1)
	go func() {
		for i := 0; i < N; i++ {
			_, _, err := ReadAndDeserialize(serverConn)
			if err != nil {
				readerDone <- err
				return
			}
		}
		readerDone <- nil
	}()

	// N 个 writer goroutine 并发写（net.Pipe 的 Write 是原子的，并发安全）
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(id int) {
			defer wg.Done()
			req := &framepb.MessageRequest{
				FuncName: "Svc.M",
				Args:     []byte{byte(id)},
			}
			if err := SendToServer(uint64(id), clientConn, req); err != nil {
				t.Errorf("并发写入错误: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if err := <-readerDone; err != nil {
		t.Fatalf("读取错误: %v", err)
	}
}

// BenchmarkSendToServer_ReadAndDeserialize 请求方向完整往返性能（net.Pipe）
// 用一个 reader goroutine 持续消费，避免 pipe 阻塞
func BenchmarkSendToServer_ReadAndDeserialize(b *testing.B) {
	b.ReportAllocs()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{8, 3, 16, 5},
	}

	go func() {
		for {
			_, _, err := ReadAndDeserialize(serverConn)
			if err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := SendToServer(uint64(i), clientConn, req); err != nil {
			b.Fatalf("SendToServer错误: %v", err)
		}
	}
}

// BenchmarkProtoMarshalUnmarshal 纯 protobuf 序列化反序列化性能基线
func BenchmarkProtoMarshalUnmarshal(b *testing.B) {
	b.ReportAllocs()
	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{8, 3, 16, 5},
		Metadata: map[string]string{"trace-id": "abc"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, _ := proto.Marshal(req)
		var decoded framepb.MessageRequest
		proto.Unmarshal(buf, &decoded)
	}
}
