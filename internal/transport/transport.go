package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/protocol"

	"google.golang.org/protobuf/proto"
)

// MetadataKeyDeadline 是 client/server 双方约定的 metadata key，
// 用于透传 context.Deadline。改为常量避免两边字符串拼写不一致。
const MetadataKeyDeadline = "deadline"

// SetDeadlineMetadata 把 deadline 序列化到 metadata，供 client 端调用。
// 单位：纳秒（绝对时间戳）。
func SetDeadlineMetadata(metadata map[string]string, deadline time.Time) {
	metadata[MetadataKeyDeadline] = strconv.FormatInt(deadline.UnixNano(), 10)
}

// ParseDeadline 从 metadata 中解析出 deadline，供 server 端调用。
// 返回绝对时间；如果没有该 key 或解析失败返回 ok=false。
func ParseDeadline(metadata map[string]string) (time.Time, bool) {
	v, ok := metadata[MetadataKeyDeadline]
	if !ok {
		return time.Time{}, false
	}
	ns, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ns <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

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
	bytes, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("[transport.go]序列化错误:%w", err)
	}
	protocol.AddHeadersBeforeBytes(requestId, &bytes)

	_, err = conn.Write(bytes)
	if err != nil {
		return fmt.Errorf("[transport.go]发送数据错误:%w", err)
	}
	return nil
}
