package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// TestClient_MaxInflight 验证背压槽位满后，新调用会等待 ctx。
func TestClient_MaxInflight(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen失败: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	defer close(done)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-done
	}()

	c := NewRPCClientWithAddr(ln.Addr().String())
	c.slots = make(chan struct{}, 1)
	defer c.Close()

	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()
	if _, err := c.CallAsync(ctx1, "Svc.M", nil); err != nil {
		t.Fatalf("第一次 CallAsync 失败: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	start := time.Now()
	if _, err := c.CallAsync(ctx2, "Svc.M", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("期望 context deadline exceeded, 得到 %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("背压等待未及时返回，耗时 %v", elapsed)
	}
}
