package server

import (
	"encoding/binary"
	"net"
	"testing"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/middleware"
	"mini-rpc/internal/protocol"
	"google.golang.org/protobuf/proto"
)

type benchServer struct{}

func (s *benchServer) Add(body []byte) ([]byte, error) {
	return proto.Marshal(&framepb.MessageResponse{
		Returns: []byte{8, 8},
		Error:   "",
	})
}

func BenchmarkSingleConnection(b *testing.B) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &benchServer{})

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go srv.handleRequest(serverConn)

	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{8, 3, 16, 5},
	}
	body, _ := proto.Marshal(req)

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
	binary.BigEndian.PutUint32(header[11:15], uint32(len(body)))

	packet := append(header[:], body...)

	respHeader := make([]byte, 15)
	respBody := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientConn.Write(packet)

		_, err := clientConn.Read(respHeader)
		if err != nil {
			b.Fatalf("读响应头失败:%v", err)
		}
		bodyLen := binary.BigEndian.Uint32(respHeader[11:15])
		_, err = clientConn.Read(respBody[:bodyLen])
		if err != nil {
			b.Fatalf("读响应体失败:%v", err)
		}
	}
}

func BenchmarkParallelClients(b *testing.B) {
	srv := NewRPCServer()
	srv.RegisterService("Calc", &benchServer{})

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go srv.handleRequest(serverConn)

	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{8, 3, 16, 5},
	}
	body, _ := proto.Marshal(req)

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
	binary.BigEndian.PutUint32(header[11:15], uint32(len(body)))

	packet := append(header[:], body...)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		respHeader := make([]byte, 15)
		respBody := make([]byte, 1024)
		for pb.Next() {
			clientConn.Write(packet)
			clientConn.Read(respHeader)
			bodyLen := binary.BigEndian.Uint32(respHeader[11:15])
			clientConn.Read(respBody[:bodyLen])
		}
	})
}

func BenchmarkProtocolHeader(b *testing.B) {
	body := []byte("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protocol.AddHeadersBeforeBytes(uint64(i), &body)
		protocol.ReadMsg(body)
	}
}

func BenchmarkProtoMarshal(b *testing.B) {
	req := &framepb.MessageRequest{
		FuncName: "Calc.Add",
		Args:     []byte{8, 3, 16, 5},
		Metadata: map[string]string{"trace-id": "abc"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes, _ := proto.Marshal(req)
		var decoded framepb.MessageRequest
		proto.Unmarshal(bytes, &decoded)
	}
}

func BenchmarkMiddlewareChain(b *testing.B) {
	handler := middleware.HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("ok")}
	})

	handler = middleware.Logger(handler)
	handler = middleware.Recovery(handler)

	req := &framepb.MessageRequest{FuncName: "Calc.Add"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler(req)
	}
}