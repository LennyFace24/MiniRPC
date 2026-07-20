package protocol

import (
	"testing"
)

func TestAddHeadersBeforeBytes(t *testing.T) {
	body := []byte("hello")
	AddHeadersBeforeBytes(42, &body)

	if len(body) != 15+5 {
		t.Fatalf("期望长度20, 得到%d", len(body))
	}

	if body[0] != 0xCC {
		t.Fatalf("Magic字节错误: 期望0xCC, 得到0x%X", body[0])
	}
	if body[1] != 0x01 {
		t.Fatalf("Version错误: 期望0x01, 得到0x%X", body[1])
	}
	if body[2] != 0x01 {
		t.Fatalf("MessageType错误: 期望0x01, 得到0x%X", body[2])
	}
}

func TestReadMsg_Ok(t *testing.T) {
	data := make([]byte, 15+10)
	data[0] = 0xCC
	data[1] = 0x01
	data[2] = 0x01
	// RequestID = 99
	data[3] = 0
	data[4] = 0
	data[5] = 0
	data[6] = 0
	data[7] = 0
	data[8] = 0
	data[9] = 0
	data[10] = 99
	// BodyLen = 10
	data[11] = 0
	data[12] = 0
	data[13] = 0
	data[14] = 10

	body, requestID, err := ReadMsg(data)
	if err != nil {
		t.Fatalf("ReadMsg返回错误:%v", err)
	}
	if requestID != 99 {
		t.Fatalf("RequestID错误: 期望99, 得到%d", requestID)
	}
	if len(body) != 10 {
		t.Fatalf("body长度错误: 期望10, 得到%d", len(body))
	}
}

func TestReadMsg_ShortHeader(t *testing.T) {
	_, _, err := ReadMsg([]byte{0xCC, 0x01})
	if err == nil {
		t.Fatal("期望长度不足错误, 但没有返回错误")
	}
}

func TestCheckProtocolHeader_Ok(t *testing.T) {
	data := make([]byte, 15)
	data[0] = 0xCC
	data[1] = 0x01
	data[2] = 0x01

	ok, err := CheckProtocolHeader(data)
	if err != nil {
		t.Fatalf("期望无错误, 得到:%v", err)
	}
	if !ok {
		t.Fatal("期望返回true")
	}
}

func TestCheckProtocolHeader_WrongMagic(t *testing.T) {
	data := make([]byte, 15)
	data[0] = 0xAA // 错误 Magic
	data[1] = 0x01
	data[2] = 0x01

	_, err := CheckProtocolHeader(data)
	if err == nil {
		t.Fatal("期望 Magic 不匹配错误, 但没有返回")
	}
}

func TestCheckProtocolHeader_ShortLength(t *testing.T) {
	_, err := CheckProtocolHeader([]byte{0xCC, 0x01})
	if err == nil {
		t.Fatal("期望长度不足错误, 但没有返回")
	}
}

func TestRoundTrip(t *testing.T) {
	body := []byte("test data")
	AddHeadersBeforeBytes(7, &body)

	gotBody, gotID, err := ReadMsg(body)
	if err != nil {
		t.Fatalf("ReadMsg错误:%v", err)
	}
	if gotID != 7 {
		t.Fatalf("RequestID: 期望7, 得到%d", gotID)
	}
	if string(gotBody) != "test data" {
		t.Fatalf("body: 期望'test data', 得到'%s'", gotBody)
	}
}