package server

import (
	"log"
	"net"
	"reflect"
	"strings"

	framepb "mini-rpc/internal/codec/proto/framepb"
	"mini-rpc/internal/middleware"
	"mini-rpc/internal/transport"
	"mini-rpc/internal/types"
)

func NewRPCServer() *RPCServer {
	return &RPCServer{functions: make(map[string]types.Function)}
}

type RPCServer struct {
	functions  map[string]types.Function
	middleware []middleware.Middleware
}

func (s *RPCServer) RegisterFunction(rcvr interface{}) {
	t := reflect.TypeOf(rcvr)
	v := reflect.ValueOf(rcvr)
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		methodValue := v.Method(i)

		// 检查签名：func(body []byte) ([]byte, error)
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

		s.functions[method.Name] = types.Function{
			Name: method.Name,
			Call: func(body []byte) ([]byte, error) {
				out := methodValue.Call([]reflect.Value{reflect.ValueOf(body)})
				if e, _ := out[1].Interface().(error); e != nil {
					return nil, e
				}
				return out[0].Bytes(), nil
			},
		}
	}
}

func (s *RPCServer) handleRequest(conn net.Conn) {
	realhandler := middleware.HandlerFunc(func(req *framepb.MessageRequest) *framepb.MessageResponse {
		function, ok := s.functions[req.FuncName]
		if !ok {
			log.Printf("[server.go]函数未注册:%v", req.FuncName)
			return &framepb.MessageResponse{Error: "函数未注册"}
		}

		result, err := function.Call(req.Args)
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
		for i := 0; i < len(s.middleware); i++ {
			handler = s.middleware[i](handler)
		}
	}

	defer conn.Close()
	for {
		data, requestID, err := transport.ReadAndDeserialize(conn)
		if err != nil {
			if !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "closed") {
				log.Printf("[server.go]读取客户端请求错误:%v", err)
			}
			return
		}
		res := handler(data)
		err = transport.SendToClient(requestID, conn, res)
		if err != nil {
			log.Printf("[server.go]发送响应数据错误:%v", err)
			continue
		}
	}
}

func (s *RPCServer) Start(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[server.go]接受客户端连接错误:%v", err)
			continue
		}
		go s.handleRequest(conn)
	}
}

func (s *RPCServer) Use(mw middleware.Middleware) {
	s.middleware = append(s.middleware, mw)
}