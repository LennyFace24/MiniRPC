package middleware

import (
	"testing"

	framepb "mini-rpc/internal/codec/proto/framepb"
)

func TestMiddlewareChain_Order(t *testing.T) {
	var order []string

	mw1 := Middleware(func(next HandlerFunc) HandlerFunc {
		return func(req *framepb.MessageRequest) *framepb.MessageResponse {
			order = append(order, "mw1_before")
			resp := next(req)
			order = append(order, "mw1_after")
			return resp
		}
	})

	mw2 := Middleware(func(next HandlerFunc) HandlerFunc {
		return func(req *framepb.MessageRequest) *framepb.MessageResponse {
			order = append(order, "mw2_before")
			resp := next(req)
			order = append(order, "mw2_after")
			return resp
		}
	})

	handler := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		order = append(order, "handler")
		return &framepb.MessageResponse{Returns: []byte("ok")}
	})

	// 先注册的在外层：mw1(mw2(handler)) → mw1 先执行 before
	handler = mw1(handler)
	handler = mw2(handler)

	req := &framepb.MessageRequest{FuncName: "test"}
	resp := handler(req)

	if string(resp.Returns) != "ok" {
		t.Fatalf("handler返回值错误: %s", resp.Returns)
	}

	expected := []string{"mw2_before", "mw1_before", "handler", "mw1_after", "mw2_after"}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("位置%d: 期望%s, 得到%s", i, v, order[i])
		}
	}
}
func TestRecoveryMiddleware_Panic(t *testing.T) {
	handler := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		panic("test panic")
	})

	handler = Recovery(handler)

	req := &framepb.MessageRequest{FuncName: "test"}
	resp := handler(req)

	if resp.Error == "" {
		t.Fatal("期望错误信息, 但没有")
	}
	if resp.Error != "panic:test panic" {
		t.Fatalf("错误信息: 期望'panic:test panic', 得到'%s'", resp.Error)
	}
}

func TestRecoveryMiddleware_Normal(t *testing.T) {
	handler := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("ok")}
	})

	handler = Recovery(handler)

	req := &framepb.MessageRequest{FuncName: "test"}
	resp := handler(req)

	if resp.Error != "" {
		t.Fatalf("期望无错误, 得到:%s", resp.Error)
	}
	if string(resp.Returns) != "ok" {
		t.Fatalf("返回值: 期望'ok', 得到'%s'", resp.Returns)
	}
}

func TestEmptyMiddlewareChain(t *testing.T) {
	handler := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("direct")}
	})

	req := &framepb.MessageRequest{FuncName: "test"}
	resp := handler(req)

	if string(resp.Returns) != "direct" {
		t.Fatalf("期望直接调用成功, 得到%s", resp.Returns)
	}
}