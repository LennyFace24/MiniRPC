package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/transport"
)

// TestClient_OutOfOrderResponses 验证同一连接上多个 in-flight 请求的响应
// 即使乱序返回，也能通过 requestID 正确匹配到各自的 Future。
func TestClient_OutOfOrderResponses(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen失败: %v", err)
	}
	defer ln.Close()

	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseFirst) })
	}
	defer release()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		_, id1, err := transport.ReadAndDeserialize(conn)
		if err != nil {
			serverDone <- err
			return
		}
		_, id2, err := transport.ReadAndDeserialize(conn)
		if err != nil {
			serverDone <- err
			return
		}

		// 先返回第二个请求，证明客户端不是依赖响应顺序匹配。
		if err := transport.SendToClient(id2, conn, &framepb.MessageResponse{
			Returns: []byte("second"),
		}); err != nil {
			serverDone <- err
			return
		}

		// 等客户端确认收到第二个响应后，再返回第一个。
		<-releaseFirst
		if err := transport.SendToClient(id1, conn, &framepb.MessageResponse{
			Returns: []byte("first"),
		}); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	c := NewRPCClientWithAddr(ln.Addr().String())
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	f1, err := c.CallAsync(ctx, "Calc.Add", []byte("first"))
	if err != nil {
		t.Fatalf("第一次 CallAsync 失败: %v", err)
	}
	f2, err := c.CallAsync(ctx, "Calc.Add", []byte("second"))
	if err != nil {
		t.Fatalf("第二次 CallAsync 失败: %v", err)
	}

	got2, err := f2.Get(ctx)
	if err != nil {
		t.Fatalf("f2.Get 失败: %v", err)
	}
	if string(got2) != "second" {
		t.Fatalf("f2 结果: 期望 second, 得到 %q", got2)
	}

	release()

	got1, err := f1.Get(ctx)
	if err != nil {
		t.Fatalf("f1.Get 失败: %v", err)
	}
	if string(got1) != "first" {
		t.Fatalf("f1 结果: 期望 first, 得到 %q", got1)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("fake server 错误: %v", err)
	}
}
