package protocol

import (
	"encoding/binary"
	"fmt"
)

// 协议头：
// Magic:1 byte 0xCC
// Version:1 byte
// MessageType:1 byte
// RequestID:8 byte
// BodyLeng:4 byte
// Total:15 byte

func AddHeadersBeforeBytes(b *[]byte) {
	var header [15]byte
	header[0] = 0xCC                                           // Magic
	header[1] = 0x01                                           // Version
	header[2] = 0x01                                           // MessageType
	binary.BigEndian.PutUint32(header[11:15], uint32(len(*b))) // BodyLen
	// RequestID, BodyLength, Total 等字段需要根据实际情况填充
	*b = append(header[:], *b...)
}

// handle the data received from the client, and return the response data
func ReadMsg(data []byte) ([]byte, error) {
	// 检查数据长度是否足够
	_, err := CheckProtocolHeader(data)
	if err != nil {
		return nil, err
	}
	// 获得协议之后得数据
	return data[15:], nil
}

func CheckProtocolHeader(data []byte) (bool, error) {
	if len(data) < 15 {
		return false, fmt.Errorf("[protocol.go] 数组长度不足，说明连协议头都未携带")
	}
	// 检查协议头是否匹配
	if (data[0] != 0xCC) || (data[1] != 0x01) || (data[2] != 0x01) {
		return false, fmt.Errorf("[protocol.go] 协议头不匹配，数据不是按照协议发送的")
	}
	return true, nil
}
