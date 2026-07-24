package server

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"reflect"
	"strings"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/middleware"
	"mini-rpc/internal/transport"
	"mini-rpc/internal/types"
)

func NewRPCServer() *RPCServer {
	return &RPCServer{services: make(map[string]*types.ServiceDesc)}
}

type RPCServer struct {
	services   map[string]*types.ServiceDesc
	middleware []middleware.Middleware
}

func (s *RPCServer) RegisterService(serviceName string, rcvr interface{}) {
	svc := &types.ServiceDesc{
		ServiceName: serviceName,
		Methods:     make(map[string]*types.MethodDesc),
	}

	t := reflect.TypeOf(rcvr)
	v := reflect.ValueOf(rcvr)
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		methodValue := v.Method(i)

		mtype := method.Type
		if mtype.NumIn() != 2 || mtype.NumOut() != 2 {
			continue
		}
		if mtype.In(1).Kind() != reflect.Slice || mtype.In(1).Elem().Kind() != reflect.Uint8 {
			continue
		}
		if mtype.Out(0).Kind() != reflect.Slice || mtype.Out(0).Elem().Kind() != reflect.Uint8 {
			continue
		}

		svc.Methods[method.Name] = &types.MethodDesc{
			MethodName: method.Name,
			Handler: func(body []byte) ([]byte, error) {
				out := methodValue.Call([]reflect.Value{reflect.ValueOf(body)})
				if e, _ := out[1].Interface().(error); e != nil {
					return nil, e
				}
				return out[0].Bytes(), nil
			},
		}
	}

	s.services[serviceName] = svc
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

		ctx := context.Background()
		// 用 transport.ParseDeadline 解析 metadata 中的 deadline（绝对时间，纳秒）
		// 替代原来 key="timeout" 按 ms 解释的 buggy 实现
		if deadline, ok := transport.ParseDeadline(data.Metadata); ok {
			ctx, _ = context.WithDeadline(ctx, deadline)
		}
		if traceID, ok := data.Metadata["trace-id"]; ok {
			ctx = context.WithValue(ctx, "trace-id", traceID)
			log.Printf("[server.go]trace=%s 请求:%s", traceID, data.FuncName)
		}

		res := s.executeHandler(ctx, data)
		err = transport.SendToClient(requestID, conn, res)
		if err != nil {
			log.Printf("[server.go]发送响应数据错误:%v", err)
			continue
		}
	}
}

func (s *RPCServer) executeHandler(ctx context.Context, req *framepb.MessageRequest) *framepb.MessageResponse {
	realhandler := middleware.HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		parts := strings.SplitN(req.FuncName, ".", 2)
		if len(parts) != 2 {
			return &framepb.MessageResponse{Error: "函数名格式错误，应为 ServiceName.MethodName"}
		}
		serviceName := parts[0]
		methodName := parts[1]

		svc, ok := s.services[serviceName]
		if !ok {
			log.Printf("[server.go]服务未注册:%v", serviceName)
			return &framepb.MessageResponse{Error: "服务未注册: " + serviceName}
		}

		md, ok := svc.Methods[methodName]
		if !ok {
			log.Printf("[server.go]函数未注册:%v", methodName)
			return &framepb.MessageResponse{Error: "函数未注册: " + methodName}
		}

		if deadline, ok := ctx.Deadline(); ok {
			if time.Now().After(deadline) {
				return &framepb.MessageResponse{Error: "context deadline exceeded"}
			}
		}

		result, err := md.Handler(req.Args)
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
	if len(s.middleware) > 0 {
		for i := range s.middleware {
			handler = s.middleware[i](handler)
		}
	}

	return handler(req)
}

// Start 阻塞接收连接。listener 关闭后会返回（不再 continue 刷日志）。
func (s *RPCServer) Start(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			// listener 被关闭时返回，不再 continue 死循环刷日志
			log.Printf("[server.go]listener 退出:%v", err)
			return
		}
		go s.handleRequest(conn)
	}
}

func (s *RPCServer) ServeTLS(addr, certFile, keyFile string) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("[server.go]加载证书失败:%v", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	listener, err := tls.Listen("tcp", addr, config)
	if err != nil {
		log.Fatalf("[server.go]TLS监听失败:%v", err)
	}
	s.Start(listener)
}

func (s *RPCServer) Use(mw middleware.Middleware) {
	s.middleware = append(s.middleware, mw)
}