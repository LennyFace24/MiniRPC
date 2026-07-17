package protocol

import (
	"encoding/binary"
	"fmt"
)

// 协议头 (15B):
// Magic:      1 byte  0xCC
// Version:    1 byte  0x01
// MessageType:1 byte  0x01
// RequestID:  8 byte
// BodyLeng:   4 byte
// Total:      15 byte

func AddHeadersBeforeBytes(id uint64, b *[]byte) {
	var header [15]byte
	header[0] = 0xCC                                           // Magic
	header[1] = 0x01                                           // Version
	header[2] = 0x01                                           // MessageType
	binary.BigEndian.PutUint64(header[3:11], id)               // RequestID
	binary.BigEndian.PutUint32(header[11:15], uint32(len(*b))) // BodyLen
	*b = append(header[:], *b...)
}

func ReadMsg(data []byte) ([]byte, uint64, error) {
	_, err := CheckProtocolHeader(data)
	if err != nil {
		return nil, 0, err
	}
	requestID := binary.BigEndian.Uint64(data[3:11])
	return data[15:], requestID, nil
}

func CheckProtocolHeader(data []byte) (bool, error) {
	if len(data) < 15 {
		return false, fmt.Errorf("[protocol.go] 数组长度不足，说明连协议头都未携带")
	}
	if (data[0] != 0xCC) || (data[1] != 0x01) || (data[2] != 0x01) {
		return false, fmt.Errorf("[protocol.go] 协议头不匹配，数据不是按照协议发送的")
	}
	return true, nil
}