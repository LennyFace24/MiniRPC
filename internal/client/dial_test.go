package client

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestClient_DialTimeoutTLS 验证 TLS 握手阻塞时，调用方 ctx 能及时返回。
func TestClient_DialTimeoutTLS(t *testing.T) {
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
	c.tlsConfig = &tls.Config{InsecureSkipVerify: true} // 测试用，服务端不做 TLS 握手
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := c.CallAsync(ctx, "Svc.M", nil); err == nil {
		t.Fatal("期望 TLS 握手超时")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("dial 超时未生效，耗时 %v", elapsed)
	}
}

// TestClient_ConcurrentDialSingleflight 验证多个调用方只会触发一次 dial。
func TestClient_ConcurrentDialSingleflight(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen失败: %v", err)
	}
	defer ln.Close()

	var accepts atomic.Int32
	accepted := make(chan struct{}, 1)
	secondAccept := make(chan struct{}, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			if accepts.Add(1) == 1 {
				accepted <- struct{}{}
			} else {
				secondAccept <- struct{}{}
			}
			go func() {
				defer conn.Close()
				<-done
			}()
		}
	}()

	c := NewRPCClientWithAddr(ln.Addr().String())
	defer c.Close()

	const n = 10
	start := make(chan struct{})
	errs := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			<-start
			errs <- c.ensureConn(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("ensureConn 失败: %v", err)
		}
	}

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("等待 accept 超时")
	}
	select {
	case <-secondAccept:
		t.Fatal("并发调用触发了多次 dial")
	case <-time.After(100 * time.Millisecond):
	}
	if got := accepts.Load(); got != 1 {
		t.Fatalf("期望只 dial 一次, 实际 %d 次", got)
	}
}
