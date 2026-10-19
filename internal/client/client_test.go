package client

import (
	"encoding/binary"
	"net"
	"testing"

	"mini-rpc/internal/protocol"
)

// writeRawHeader 向连接写入一个只有协议头的响应帧，供 readOneResponse 校验。
func writeRawHeader(t *testing.T, conn net.Conn, header []byte) {
	t.Helper()
	go func() {
		_, _ = conn.Write(header)
	}()
}

func TestReadOneResponse_RejectsBadHeader(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	header := make([]byte, protocol.HeaderSize)
	header[0] = 0xAA // wrong magic
	header[1] = 0x01
	header[2] = 0x01
	binary.BigEndian.PutUint32(header[11:15], 0)
	writeRawHeader(t, clientConn, header)

	c := NewRPCClient()
	if _, _, err := c.readOneResponse(serverConn); err == nil {
		t.Fatal("期望协议头校验错误")
	}
}

func TestReadOneResponse_RejectsOversizedFrame(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	header := make([]byte, protocol.HeaderSize)
	header[0] = 0xCC
	header[1] = 0x01
	header[2] = 0x01
	binary.BigEndian.PutUint32(header[11:15], protocol.MaxFrameSize+1)
	writeRawHeader(t, clientConn, header)

	c := NewRPCClient()
	if _, _, err := c.readOneResponse(serverConn); err == nil {
		t.Fatal("期望消息体过大错误")
	}
}
