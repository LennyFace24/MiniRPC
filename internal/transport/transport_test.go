package transport

import (
	"net"
	"testing"

	framepb "mini-rpc/internal/codec/proto/framepb"
)

func TestReadAndDeserialize_SendToClient_RoundTrip(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	req := &framepb.MessageRequest{
		FuncName: "Calculator.Add",
		Args:     []byte{8, 3, 16, 5},
		Metadata: map[string]string{"trace-id": "abc"},
	}

	go func() {
		err := SendToClient(42, clientConn, &framepb.MessageResponse{
			Returns: []byte{8, 8},
			Error:   "",
		})
		if err != nil {
			t.Errorf("SendToClient error: %v", err)
		}
	}()

	err := SendToServer(42, serverConn, req)
	if err != nil {
		t.Fatalf("SendToServer error: %v", err)
	}

	got, gotID, err := ReadAndDeserialize(serverConn)
	if err != nil {
		t.Fatalf("ReadAndDeserialize error: %v", err)
	}
	if gotID != 42 {
		t.Fatalf("RequestID: 期望42, 得到%d", gotID)
	}
	if got.FuncName != "Calculator.Add" {
		t.Fatalf("FuncName: 期望Calculator.Add, 得到%s", got.FuncName)
	}
	if got.Metadata["trace-id"] != "abc" {
		t.Fatalf("trace-id metadata丢失")
	}
}

func TestSendToClient_Response(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go func() {
		SendToClient(7, clientConn, &framepb.MessageResponse{
			Returns: []byte{8, 8},
			Error:   "",
		})
	}()

	_, gotID, err := ReadAndDeserialize(serverConn)
	if err != nil {
		t.Fatalf("ReadAndDeserialize error: %v", err)
	}
	if gotID != 7 {
		t.Fatalf("RequestID: 期望7, 得到%d", gotID)
	}
}

func TestSendToServer_AndReadRequest(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	req := &framepb.MessageRequest{
		FuncName: "TestService.Add",
		Args:     []byte{8, 3, 16, 5},
	}

	go func() {
		err := SendToServer(99, clientConn, req)
		if err != nil {
			t.Errorf("SendToServer error: %v", err)
		}
	}()

	got, gotID, err := ReadAndDeserialize(serverConn)
	if err != nil {
		t.Fatalf("ReadAndDeserialize error: %v", err)
	}
	if gotID != 99 {
		t.Fatalf("RequestID: 期望99, 得到%d", gotID)
	}
	if got.FuncName != "TestService.Add" {
		t.Fatalf("FuncName: 期望TestService.Add, 得到%s", got.FuncName)
	}
}