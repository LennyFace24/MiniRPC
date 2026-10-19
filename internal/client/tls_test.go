package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"mini-rpc/internal/server"
)

// makeTLSCert 生成测试用自签证书。
func makeTLSCert(t *testing.T) tls.Certificate {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("生成证书失败: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("序列化私钥失败: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("加载证书失败: %v", err)
	}
	return cert
}

// TestPool_TLS_EndToEnd 验证服务端 TLS 监听与客户端 TLS 拨号的完整链路。
func TestPool_TLS_EndToEnd(t *testing.T) {
	cert := makeTLSCert(t)

	srv := server.NewRPCServer()
	srv.RegisterService("Calc", &testPoolHandler{})

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatalf("TLS Listen失败: %v", err)
	}
	defer ln.Close()
	go srv.Start(ln)

	addr := ln.Addr().String()
	pool := NewPoolTLS(1, addr, &tls.Config{
		InsecureSkipVerify: true, // 测试用自签证书
	})
	defer func() {
		for _, c := range pool.pools[addr] {
			_ = c.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	future, err := pool.CallAsync(ctx, "Calc.Add", RoundRobin, makeAddBody(3, 5))
	if err != nil {
		t.Fatalf("CallAsync 失败: %v", err)
	}
	ret, err := future.Get(ctx)
	if err != nil {
		t.Fatalf("TLS 调用失败: %v", err)
	}
	if got := parseAddResult(ret); got != 8 {
		t.Fatalf("Result: 期望8, 得到%d", got)
	}
}
