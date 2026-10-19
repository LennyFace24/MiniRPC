package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/middleware"
	"mini-rpc/internal/transport"
	"mini-rpc/internal/types"
)

type contextKey string

const traceIDKey contextKey = "trace-id"

func NewRPCServer() *RPCServer {
	return &RPCServer{services: make(map[string]*types.ServiceDesc)}
}

type RPCServer struct {
	mu         sync.RWMutex
	services   map[string]*types.ServiceDesc
	middleware []middleware.Middleware
}

func (s *RPCServer) RegisterService(serviceName string, rcvr interface{}) {
	svc := &types.ServiceDesc{
		ServiceName: serviceName,
		Methods:     make(map[string]*types.MethodDesc),
	}

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	bytesType := reflect.TypeOf([]byte(nil))
	errorType := reflect.TypeOf((*error)(nil)).Elem()

	t := reflect.TypeOf(rcvr)
	v := reflect.ValueOf(rcvr)
	for i := range t.NumMethod() {
		method := t.Method(i)
		methodValue := v.Method(i)

		mtype := method.Type
		if mtype.NumIn() != 3 || mtype.NumOut() != 2 {
			continue
		}
		if mtype.In(1) != ctxType {
			continue
		}
		if mtype.In(2) != bytesType {
			continue
		}
		if mtype.Out(0) != bytesType || !mtype.Out(1).Implements(errorType) {
			continue
		}

		svc.Methods[method.Name] = &types.MethodDesc{
			MethodName: method.Name,
			Handler: func(ctx context.Context, body []byte) ([]byte, error) {
				out := methodValue.Call([]reflect.Value{
					reflect.ValueOf(ctx),
					reflect.ValueOf(body),
				})
				if e, _ := out[1].Interface().(error); e != nil {
					return nil, e
				}
				return out[0].Bytes(), nil
			},
		}
	}

	s.mu.Lock()
	s.services[serviceName] = svc
	s.mu.Unlock()
}

func (s *RPCServer) handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		data, requestID, err := transport.ReadAndDeserialize(conn)
		if err != nil {
			if !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "closed") {
				log.Printf("[server.go]读取客户端请求错误:%v", err)
			}
			return
		}

		// 用 transport.ParseDeadline 解析 metadata 中的 deadline（绝对时间，纳秒）
		// 替代原来 key="timeout" 按 ms 解释的 buggy 实现
		res := func() *framepb.MessageResponse {
			ctx := context.Background()
			var cancel context.CancelFunc = func() {}
			if deadline, ok := transport.ParseDeadline(data.Metadata); ok {
				ctx, cancel = context.WithDeadline(ctx, deadline)
			}
			defer cancel()

			if traceID, ok := data.Metadata["trace-id"]; ok {
				ctx = context.WithValue(ctx, traceIDKey, traceID)
				log.Printf("[server.go]trace=%s 请求:%s", traceID, data.FuncName)
			}

			return s.executeHandler(ctx, data)
		}()
		err = transport.SendToClient(requestID, conn, res)
		if err != nil {
			log.Printf("[server.go]发送响应数据错误:%v", err)
			continue
		}
	}
}

func (s *RPCServer) executeHandler(ctx context.Context, req *framepb.MessageRequest) *framepb.MessageResponse {
	realhandler := middleware.HandlerFunc(func(ctx context.Context, req *framepb.MessageRequest) *framepb.MessageResponse {
		parts := strings.SplitN(req.FuncName, ".", 2)
		if len(parts) != 2 {
			return &framepb.MessageResponse{Error: "函数名格式错误，应为 ServiceName.MethodName"}
		}
		serviceName := parts[0]
		methodName := parts[1]

		s.mu.RLock()
		svc, ok := s.services[serviceName]
		var md *types.MethodDesc
		if ok {
			md, ok = svc.Methods[methodName]
		}
		s.mu.RUnlock()

		if !ok {
			log.Printf("[server.go]服务或方法未注册:%v.%v", serviceName, methodName)
			return &framepb.MessageResponse{Error: "服务未注册: " + serviceName}
		}

		if deadline, ok := ctx.Deadline(); ok {
			if time.Now().After(deadline) {
				return &framepb.MessageResponse{Error: "context deadline exceeded"}
			}
		}

		result, err := md.Handler(ctx, req.Args)
		if err != nil {
			log.Printf("[server.go]调用函数错误:%v", err)
			return &framepb.MessageResponse{Error: "函数调用错误"}
		}

		return &framepb.MessageResponse{
			Returns: result,
			Error:   "",
		}
	})

	handler := realhandler
	s.mu.RLock()
	mws := append([]middleware.Middleware(nil), s.middleware...)
	s.mu.RUnlock()
	for i := range mws {
		handler = mws[i](handler)
	}

	return handler(ctx, req)
}

// Start 阻塞接收连接。listener 关闭后会返回错误。
func (s *RPCServer) Start(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			// listener 被关闭时返回，不再 continue 死循环刷日志
			log.Printf("[server.go]listener 退出:%v", err)
			return err
		}
		go s.handleRequest(conn)
	}
}

func (s *RPCServer) ServeTLS(addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("[server.go]加载证书失败:%w", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	listener, err := tls.Listen("tcp", addr, config)
	if err != nil {
		return fmt.Errorf("[server.go]TLS监听失败:%w", err)
	}
	return s.Start(listener)
}

func (s *RPCServer) Use(mw middleware.Middleware) {
	s.mu.Lock()
	s.middleware = append(s.middleware, mw)
	s.mu.Unlock()
}
