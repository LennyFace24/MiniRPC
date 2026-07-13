package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"mini-rpc/internal/codec"
	"mini-rpc/internal/config"
	"mini-rpc/internal/protocol"
	"mini-rpc/internal/types"
)

// var bufPool = sync.Pool{
// 	New: func() interface{} {
// 		return make([]byte, 4096) // 4KB buffer
// 	},
// }

func ReadAndDeserialize(conn net.Conn) (types.RequestData, uint64, error) {
	// 先读协议头
	header := make([]byte, 15)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return types.RequestData{}, 0, fmt.Errorf("[transport.go]读取协议头错误:%v", err)
	}
	// 检验协议头
	ok, err := protocol.CheckProtocolHeader(header)
	if err != nil {
		return types.RequestData{}, 0, fmt.Errorf("[transport.go]协议头校验错误:%v", err)
	}
	if !ok {
		return types.RequestData{}, 0, fmt.Errorf("[transport.go]协议头校验失败")
	}
	// 读取数据
	bodyLength := binary.BigEndian.Uint32(header[11:15])
	totalLen := 15 + bodyLength
	msg := make([]byte, totalLen)
	copy(msg, header)
	_, err = io.ReadFull(conn, msg[15:])
	if err != nil {
		return types.RequestData{}, 0, fmt.Errorf("[transport.go]读取数据错误:%v", err)
	}
	// 解析 RequestID
	requestID := binary.BigEndian.Uint64(header[3:11])
	// 反序列化
	var requestData types.RequestData
	err = codec.Deserialize(msg[15:], &requestData)

	return requestData, requestID, nil
}

func SendToClient(requestId uint64, conn net.Conn, res types.ResponseData) error {
	// 序列化
	bytes, err := codec.Serialize(res)
	if err != nil {
		return fmt.Errorf("[transport.go]序列化错误:%v", err)
	}
	// 添加协议头
	protocol.AddHeadersBeforeBytes(requestId, &bytes)
	// 发送数据
	_, err = conn.Write(bytes)
	if err != nil {
		return fmt.Errorf("[transport.go]发送数据错误:%v", err)
	}
	return nil
}

func SendToServer(requestId uint64, conn net.Conn, req types.RequestData) error {
	// 获取服务器地址
	config := config.LoadConfig()
	if config == nil {
		log.Printf("[transport.go]加载配置文件错误")
		return fmt.Errorf("[transport.go]加载配置文件错误")
	}

	// 序列化
	bytes, err := codec.Serialize(req)
	if err != nil {
		log.Printf("[transport.go]序列化错误:%v", err)
		return fmt.Errorf("[transport.go]序列化错误:%v", err)
	}
	// 添加协议头
	protocol.AddHeadersBeforeBytes(requestId, &bytes)

	_, err = conn.Write(bytes)
	if err != nil {
		log.Printf("[transport.go]发送数据错误:%v", err)
		return fmt.Errorf("[transport.go]发送数据错误:%v", err)
	}
	// 读取响应
	// respHeader := make([]byte, 15)
	// _, err = io.ReadFull(conn, respHeader)
	// if err != nil {
	// 	return nil, fmt.Errorf("[transport.go]读取响应协议头错误:%v", err)
	// }
	// // 检验协议头
	// ok, err := protocol.CheckProtocolHeader(respHeader)
	// if err != nil {
	// 	return nil, fmt.Errorf("[transport.go]响应协议头校验错误:%v", err)
	// }
	// if !ok {
	// 	return nil, fmt.Errorf("[transport.go]响应协议头校验失败")
	// }
	// // 读取响应数据
	// bodyLength := binary.BigEndian.Uint32(respHeader[11:15])
	// totalLen := 15 + bodyLength
	// respMsg := make([]byte, totalLen)
	// copy(respMsg, respHeader)
	// _, err = io.ReadFull(conn, respMsg[15:])
	// if err != nil {
	// 	return nil, fmt.Errorf("[transport.go]读取响应数据错误:%v", err)
	// }
	return nil

}
