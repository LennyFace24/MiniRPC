package server

import (
	"context"
	"net"
	"testing"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/middleware"
	"google.golang.org/protobuf/proto"
)

type testServer struct{}

func (s *testServer) Add(body []byte) ([]byte, error) {
	return proto.Marshal(&framepb.MessageResponse{
		Returns: []byte{8, 8},
		Error:   "",
	})
}

func (s *testServer) Panic(body []byte) ([]byte, error) {
	panic("test panic")
}

func TestServer_RegisterAndExecute(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Test", &testServer{})

	req := &framepb.MessageRequest{
		FuncName: "Test.Add",
		Args:     []byte{8, 3, 16, 5},
	}

	resp := srv.executeHandler(context.Background(), req)
	if resp.Error != "" {
		t.Fatalf("期望无错误, 得到:%s", resp.Error)
	}
}

func TestServer_UnregisteredService(t *testing.T) {
	srv := NewRPCServer()

	req := &framepb.MessageRequest{
		FuncName: "Unknown.Add",
		Args:     []byte{},
	}

	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望'服务未注册'错误, 但没有")
	}
}

func TestServer_UnregisteredMethod(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Test", &testServer{})

	req := &framepb.MessageRequest{
		FuncName: "Test.Unknown",
		Args:     []byte{},
	}

	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望'函数未注册'错误, 但没有")
	}
}

func TestServer_WrongFuncNameFormat(t *testing.T) {
	srv := NewRPCServer()

	req := &framepb.MessageRequest{
		FuncName: "NoDot",
		Args:     []byte{},
	}

	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望格式错误, 但没有")
	}
}

func TestServer_Timeout(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Test", &testServer{})

	req := &framepb.MessageRequest{
		FuncName: "Test.Add",
		Args:     []byte{},
		Metadata: map[string]string{"timeout": "1"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	resp := srv.executeHandler(ctx, req)
	if resp.Error == "" {
		t.Fatal("期望超时错误, 但没有")
	}
}

func TestServer_PanicRecovery(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Test", &testServer{})
	srv.Use(middleware.Recovery)

	req := &framepb.MessageRequest{
		FuncName: "Test.Panic",
		Args:     []byte{},
	}

	resp := srv.executeHandler(context.Background(), req)
	if resp.Error == "" {
		t.Fatal("期望panic被Recovery中间件捕获并返回错误")
	}
}

func TestServer_EndToEnd(t *testing.T) {
	srv := NewRPCServer()
	srv.RegisterService("Test", &testServer{})

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go srv.handleRequest(serverConn)

	req := &framepb.MessageRequest{
		FuncName: "Test.Add",
		Args:     []byte{8, 3, 16, 5},
	}
	bytes, _ := proto.Marshal(req)

	var header [15]byte
	header[0] = 0xCC
	header[1] = 0x01
	header[2] = 0x01
	header[3] = 0
	header[4] = 0
	header[5] = 0
	header[6] = 0
	header[7] = 0
	header[8] = 0
	header[9] = 0
	header[10] = 1
	header[11] = 0
	header[12] = 0
	header[13] = 0
	header[14] = byte(len(bytes))

	clientConn.Write(append(header[:], bytes...))

	respHeader := make([]byte, 15)
	_, err := clientConn.Read(respHeader)
	if err != nil {
		t.Fatalf("读取响应头失败:%v", err)
	}
	bodyLen := int(respHeader[11])<<24 | int(respHeader[12])<<16 | int(respHeader[13])<<8 | int(respHeader[14])
	respBody := make([]byte, bodyLen)
	_, err = clientConn.Read(respBody)
	if err != nil {
		t.Fatalf("读取响应体失败:%v", err)
	}

	var resp framepb.MessageResponse
	proto.Unmarshal(respBody, &resp)
	if resp.Error != "" {
		t.Fatalf("端到端调用失败:%s", resp.Error)
	}
}