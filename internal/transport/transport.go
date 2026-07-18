package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	framepb "mini-rpc/internal/codec/proto/framepb"

	"mini-rpc/internal/config"
	"mini-rpc/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func ReadAndDeserialize(conn net.Conn) (*framepb.MessageRequest, uint64, error) {
	header := make([]byte, 15)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, 0, fmt.Errorf("[transport.go]读取协议头错误:%v", err)
	}
	ok, err := protocol.CheckProtocolHeader(header)
	if err != nil {
		return nil, 0, fmt.Errorf("[transport.go]协议头校验错误:%v", err)
	}
	if !ok {
		return nil, 0, fmt.Errorf("[transport.go]协议头校验失败")
	}
	bodyLength := binary.BigEndian.Uint32(header[11:15])
	totalLen := 15 + bodyLength
	msg := make([]byte, totalLen)
	copy(msg, header)
	_, err = io.ReadFull(conn, msg[15:])
	if err != nil {
		return nil, 0, fmt.Errorf("[transport.go]读取数据错误:%v", err)
	}
	requestID := binary.BigEndian.Uint64(header[3:11])
	var requestData framepb.MessageRequest
	err = proto.Unmarshal(msg[15:], &requestData)
	if err != nil {
		return nil, 0, fmt.Errorf("[transport.go]反序列化错误:%v", err)
	}
	return &requestData, requestID, nil
}

func SendToClient(requestId uint64, conn net.Conn, res *framepb.MessageResponse) error {
	bytes, err := proto.Marshal(res)
	if err != nil {
		return fmt.Errorf("[transport.go]序列化错误:%v", err)
	}
	protocol.AddHeadersBeforeBytes(requestId, &bytes)
	_, err = conn.Write(bytes)
	if err != nil {
		return fmt.Errorf("[transport.go]发送数据错误:%v", err)
	}
	return nil
}

func SendToServer(requestId uint64, conn net.Conn, req *framepb.MessageRequest) error {
	config := config.LoadConfig()
	if config == nil {
		log.Printf("[transport.go]加载配置文件错误")
		return fmt.Errorf("[transport.go]加载配置文件错误")
	}

	bytes, err := proto.Marshal(req)
	if err != nil {
		log.Printf("[transport.go]序列化错误:%v", err)
		return fmt.Errorf("[transport.go]序列化错误:%v", err)
	}
	protocol.AddHeadersBeforeBytes(requestId, &bytes)

	_, err = conn.Write(bytes)
	if err != nil {
		log.Printf("[transport.go]发送数据错误:%v", err)
		return fmt.Errorf("[transport.go]发送数据错误:%v", err)
	}
	return nil
}