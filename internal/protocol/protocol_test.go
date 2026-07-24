package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestAddHeadersBeforeBytes 验证协议头写入是否正确覆盖所有字段
func TestAddHeadersBeforeBytes(t *testing.T) {
	body := []byte("hello")
	AddHeadersBeforeBytes(42, &body)

	if len(body) != 15+5 {
		t.Fatalf("总长度: 期望20, 得到%d", len(body))
	}
	if body[0] != 0xCC {
		t.Fatalf("Magic: 期望0xCC, 得到0x%X", body[0])
	}
	if body[1] != 0x01 {
		t.Fatalf("Version: 期望0x01, 得到0x%X", body[1])
	}
	if body[2] != 0x01 {
		t.Fatalf("MessageType: 期望0x01, 得到0x%X", body[2])
	}
	if got := binary.BigEndian.Uint64(body[3:11]); got != 42 {
		t.Fatalf("RequestID: 期望42, 得到%d", got)
	}
	if got := binary.BigEndian.Uint32(body[11:15]); got != 5 {
		t.Fatalf("BodyLen: 期望5, 得到%d", got)
	}
	if string(body[15:]) != "hello" {
		t.Fatalf("body内容: 期望'hello', 得到'%s'", body[15:])
	}
}

// TestAddHeaders_ZeroBody 验证空 body 的边界情况
func TestAddHeaders_ZeroBody(t *testing.T) {
	body := []byte{}
	AddHeadersBeforeBytes(0, &body)
	if len(body) != 15 {
		t.Fatalf("空body总长度: 期望15, 得到%d", len(body))
	}
	if binary.BigEndian.Uint32(body[11:15]) != 0 {
		t.Fatal("空body的BodyLen应为0")
	}
}

// TestAddHeaders_MaxRequestID 验证 RequestID 最大值
func TestAddHeaders_MaxRequestID(t *testing.T) {
	body := []byte("x")
	const maxID = ^uint64(0)
	AddHeadersBeforeBytes(maxID, &body)
	if got := binary.BigEndian.Uint64(body[3:11]); got != maxID {
		t.Fatalf("RequestID: 期望%d, 得到%d", maxID, got)
	}
}

// TestReadMsg_Success 验证完整消息读取
func TestReadMsg_Success(t *testing.T) {
	original := []byte("payload")
	msg := make([]byte, 15+len(original))
	msg[0] = 0xCC
	msg[1] = 0x01
	msg[2] = 0x01
	binary.BigEndian.PutUint64(msg[3:11], 123)
	binary.BigEndian.PutUint32(msg[11:15], uint32(len(original)))
	copy(msg[15:], original)

	body, requestID, err := ReadMsg(msg)
	if err != nil {
		t.Fatalf("ReadMsg错误:%v", err)
	}
	if requestID != 123 {
		t.Fatalf("RequestID: 期望123, 得到%d", requestID)
	}
	if !bytes.Equal(body, original) {
		t.Fatalf("body: 期望%v, 得到%v", original, body)
	}
}

// TestReadMsg_ShortHeader 验证长度不足时返回错误
func TestReadMsg_ShortHeader(t *testing.T) {
	_, _, err := ReadMsg([]byte{0xCC, 0x01})
	if err == nil {
		t.Fatal("期望长度不足错误, 但未返回")
	}
}

// TestCheckProtocolHeader 校验各种协议头情况
func TestCheckProtocolHeader(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{"正常", []byte{0xCC, 0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false},
		{"Magic错误", []byte{0xAA, 0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, true},
		{"Version错误", []byte{0xCC, 0x02, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, true},
		{"MessageType错误", []byte{0xCC, 0x01, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, true},
		{"长度不足", []byte{0xCC, 0x01}, true},
		{"空数据", []byte{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := CheckProtocolHeader(tt.data)
			if tt.wantErr {
				if err == nil {
					t.Fatal("期望错误, 但未返回")
				}
				if ok {
	
				t.Fatal("有错误时不应返回true")
				}
			} else {
				if err != nil {
					t.Fatalf("期望无错误, 得到:%v", err)
				}
				if !ok {
					t.Fatal("期望返回true")
				}
			}
		})
	}
}

// TestRoundTrip 验证编码后能完整解码回来
func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		id   uint64
		body []byte
	}{
		{"普通", 7, []byte("test data")},
		{"空body", 100, []byte{}},
		{"大ID", ^uint64(0), []byte("max id")},
		{"二进制数据", 42, []byte{0x00, 0xFF, 0xCC, 0x01, 0x02}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, len(tt.body))
			copy(data, tt.body)
			AddHeadersBeforeBytes(tt.id, &data)

			gotBody, gotID, err := ReadMsg(data)
			if err != nil {
				t.Fatalf("ReadMsg错误:%v", err)
			}
			if gotID != tt.id {
				t.Fatalf("RequestID: 期望%d, 得到%d", tt.id, gotID)
			}
			if !bytes.Equal(gotBody, tt.body) {
				t.Fatalf("body: 期望%v, 得到%v", tt.body, gotBody)
			}
		})
	}
}

// BenchmarkAddHeaders 协议头写入性能
func BenchmarkAddHeaders(b *testing.B) {
	b.ReportAllocs()
	body := make([]byte, 64)
	for i := range body {
		body[i] = byte(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, len(body))
		copy(buf, body)
		AddHeadersBeforeBytes(uint64(i), &buf)
	}
}

// BenchmarkReadMsg 协议头读取性能
func BenchmarkReadMsg(b *testing.B) {
	b.ReportAllocs()
	body := make([]byte, 64)
	for i := range body {
		body[i] = byte(i)
	}
	AddHeadersBeforeBytes(1, &body)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ReadMsg(body)
	}
}
