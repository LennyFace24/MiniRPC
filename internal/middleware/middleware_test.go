package middleware

import (
	"strings"
	"testing"

	framepb "mini-rpc/internal/codec/proto/framepb"
)

// recordingMiddleware 返回一个中间件，记录 before/after 的执行顺序
func recordingMiddleware(name string, order *[]string) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(req *framepb.MessageRequest) *framepb.MessageResponse {
			*order = append(*order, name+"_before")
			resp := next(req)
			*order = append(*order, name+"_after")
			return resp
		}
	}
}

// TestMiddlewareChain_Order 验证中间件链的嵌套执行顺序
// 包装顺序 mw1(mw2(handler))：mw1 在最外层，先执行 before，最后执行 after
func TestMiddlewareChain_Order(t *testing.T) {
	var order []string

	mw1 := recordingMiddleware("mw1", &order)
	mw2 := recordingMiddleware("mw2", &order)

	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		order = append(order, "handler")
		return &framepb.MessageResponse{Returns: []byte("ok")}
	})

	// 模拟 server.go 中的组装逻辑：for i := range s.middleware { handler = mw[i](handler) }
	// 注册顺序 [mw1, mw2] → mw2 在最外层
	handler := base
	handler = mw1(handler) // mw1(base)
	handler = mw2(handler) // mw2(mw1(base))

	resp := handler(&framepb.MessageRequest{FuncName: "test"})

	if string(resp.Returns) != "ok" {
		t.Fatalf("返回值: 期望'ok', 得到'%s'", resp.Returns)
	}

	expected := []string{"mw2_before", "mw1_before", "handler", "mw1_after", "mw2_after"}
	if len(order) != len(expected) {
		t.Fatalf("执行顺序长度: 期望%d, 得到%d (%v)", len(expected), len(order), order)
	}
	for i, want := range expected {
		if order[i] != want {
			t.Fatalf("位置%d: 期望%s, 得到%s", i, want, order[i])
		}
	}
}

// TestMiddlewareChain_RegistrationOrder 验证不同注册顺序产生不同执行顺序
// 这是 server.Use 的关键语义：最后注册的在最外层
func TestMiddlewareChain_RegistrationOrder(t *testing.T) {
	run := func(regOrder []string) []string {
		var order []string
		mws := map[string]Middleware{
			"A": recordingMiddleware("A", &order),
			"B": recordingMiddleware("B", &order),
		}
		base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
			order = append(order, "handler")
			return &framepb.MessageResponse{}
		})
		handler := base
		for _, name := range regOrder {
			handler = mws[name](handler)
		}
		handler(&framepb.MessageRequest{})
		return order
	}

	// 注册 [A, B] → B 在最外层
	gotAB := run([]string{"A", "B"})
	wantAB := []string{"B_before", "A_before", "handler", "A_after", "B_after"}
	if len(gotAB) != len(wantAB) {
		t.Fatalf("[A,B] 顺序长度不一致: %v", gotAB)
	}
	for i := range wantAB {
		if gotAB[i] != wantAB[i] {
			t.Fatalf("[A,B] 位置%d: 期望%s, 得到%s", i, wantAB[i], gotAB[i])
		}
	}

	// 注册 [B, A] → A 在最外层
	gotBA := run([]string{"B", "A"})
	wantBA := []string{"A_before", "B_before", "handler", "B_after", "A_after"}
	if len(gotBA) != len(wantBA) {
		t.Fatalf("[B,A] 顺序长度不一致: %v", gotBA)
	}
	for i := range wantBA {
		if gotBA[i] != wantBA[i] {
			t.Fatalf("[B,A] 位置%d: 期望%s, 得到%s", i, wantBA[i], gotBA[i])
	
	}
	}
}

// TestRecoveryMiddleware_Panic 验证 panic 被捕获并转为 error 响应
func TestRecoveryMiddleware_Panic(t *testing.T) {
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		panic("test panic")
	})
	handler := Recovery(base)

	resp := handler(&framepb.MessageRequest{FuncName: "test"})

	if resp == nil {
		t.Fatal("期望非nil响应, 但得到nil")
	}
	if resp.Error == "" {
		t.Fatal("期望错误信息, 但没有")
	}
	if !strings.Contains(resp.Error, "test panic") {
		t.Fatalf("错误信息应包含'test panic', 得到'%s'", resp.Error)
	}
}

// TestRecoveryMiddleware_PanicWithStruct 验证 panic 各种类型都能被捕获
func TestRecoveryMiddleware_PanicWithStruct(t *testing.T) {
	type customError struct{ msg string }
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		panic(customError{msg: "custom panic"})
	})
	handler := Recovery(base)

	resp := handler(&framepb.MessageRequest{})
	if resp.Error == "" {
		t.Fatal("期望错误信息, 但没有")
	}
}

// TestRecoveryMiddleware_Normal 验证正常请求不被中间件影响
func TestRecoveryMiddleware_Normal(t *testing.T) {
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("ok")}
	})
	handler := Recovery(base)

	resp := handler(&framepb.MessageRequest{FuncName: "test"})
	if resp.Error != "" {
		t.Fatalf("期望无错误, 得到:%s", resp.Error)
	}
	if string(resp.Returns) != "ok" {
		t.Fatalf("返回值: 期望'ok', 得到'%s'", resp.Returns)
	}
}

// TestRecoveryMiddleware_NilPanic 验证 panic(nil) 边界情况
func TestRecoveryMiddleware_NilPanic(t *testing.T) {
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		panic(nil)
	})
	handler := Recovery(base)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Recovery 应捕获 panic(nil), 但逃逸了: %v", r)
		}
	}()
	resp := handler(&framepb.MessageRequest{})
	if resp == nil {
		t.Fatal("期望非nil响应")
	}
}

// TestEmptyMiddlewareChain 验证无中间件时 handler 直接执行
func TestEmptyMiddlewareChain(t *testing.T) {
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("direct")}
	})
	resp := base(&framepb.MessageRequest{FuncName: "test"})
	if string(resp.Returns) != "direct" {
		t.Fatalf("期望'direct', 得到%s", resp.Returns)
	}
}

// TestLoggerMiddleware 验证 Logger 中间件透传请求和响应
func TestLoggerMiddleware(t *testing.T) {
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("logged")}
	})
	handler := Logger(base)

	resp := handler(&framepb.MessageRequest{FuncName: "Svc.M"})
	if resp == nil {
		t.Fatal("期望非nil响应")
	}
	if string(resp.Returns) != "logged" {
		t.Fatalf("返回值: 期望'logged', 得到'%s'", resp.Returns)
	}
}

// TestLoggerMiddleware_WithPanic 验证 Logger 与 Recovery 组合时 panic 不影响 Logger
func TestLoggerMiddleware_WithPanic(t *testing.T) {
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		panic("oops")
	})
	// 模拟 server 组装：注册 [Logger, Recovery] → Recovery 在最外层
	handler := Logger(base)
	handler = Recovery(handler)

	resp := handler(&framepb.MessageRequest{FuncName: "test"})
	if resp == nil || resp.Error == "" {
		t.Fatal("期望 panic 被捕获并返回 error 响应")
	}
}

// BenchmarkRecoveryMiddleware Recovery 中间件性能（无 panic 路径）
func BenchmarkRecoveryMiddleware(b *testing.B) {
	b.ReportAllocs()
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("ok")}
	})
	handler := Recovery(base)
	req := &framepb.MessageRequest{FuncName: "Svc.M"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler(req)
	}
}

// BenchmarkMiddlewareChain 多层中间件链性能（用无 IO 的 recordingMiddleware，避免 Logger 干扰）
func BenchmarkMiddlewareChain(b *testing.B) {
	b.ReportAllocs()
	var order []string
	mw1 := recordingMiddleware("mw1", &order)
	mw2 := recordingMiddleware("mw2", &order)
	base := HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		return &framepb.MessageResponse{Returns: []byte("ok")}
	})
	handler := mw1(base)
	handler = mw2(handler)
	req := &framepb.MessageRequest{FuncName: "Svc.M"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler(req)
		// 每次清空，避免 order 无限增长影响内存测量
		order = order[:0]
	}
}
