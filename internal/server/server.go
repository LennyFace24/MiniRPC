package server

import (
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"

	"mini-rpc/internal/middleware"
	"mini-rpc/internal/transport"
	"mini-rpc/internal/types"
)

func NewRPCServer() *RPCServer {
	return &RPCServer{functions: make(map[string]types.Function)}
}

// server
type RPCServer struct {
	functions  map[string]types.Function
	middleware []middleware.Middleware
}

// 注册函数
func (s *RPCServer) RegisterFunction(rcvr interface{}) {

	// Implementation for registering functions
	t := reflect.TypeOf(rcvr)  // 类型type
	v := reflect.ValueOf(rcvr) // 值value
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		// method.Name,method.Type
		// 给每个方法生成一个 Call 闭包
		s.functions[method.Name] = types.Function{
			Name: method.Name,
			Call: func(args ...interface{}) ([]interface{}, error) {
				// 1.函数传参
				in := []reflect.Value{v}
				for _, arg := range args {
					in = append(in, reflect.ValueOf(arg))
				}
				// 2. 调用函数
				out := method.Func.Call(in)

				// 3. 处理返回值
				if len(out) > 0 {
					if e, _ := out[len(out)-1].Interface().(error); e != nil {
						out = out[:len(out)-1] // 去掉最后一个 error
						return nil, fmt.Errorf("[server.go]函数调用错误:%v", e)
					}
				}

				// 4. 转成 []interface{}
				result := make([]interface{}, len(out))
				for i, vv := range out {
					result[i] = vv.Interface()
				}
				return result, nil
			},
		}
	}
}

// 接收客户端的函数调用请求
func (s *RPCServer) handleRequest(conn net.Conn) {
	realhandler := func(req types.RequestData) types.ResponseData {
		function, ok := s.functions[req.FuncName]
		if !ok {
			log.Printf("[server.go]函数未注册:%v", req.FuncName)
			return types.ResponseData{Error: "函数未注册"}
		}

		result, err := function.Call(req.Arguments...)
		if err != nil {
			log.Printf("[server.go]调用函数错误:%v", err)
			return types.ResponseData{Error: "函数调用错误"}
		}

		// 封装res，响应结果
		res := types.ResponseData{
			Returns: result,
			Error:   "",
		}
		return res
	}

	handler := realhandler

	if len(s.middleware) > 0 {
		// 嵌套中间件
		for i := 0; i < len(s.middleware); i++ {
			handler = s.middleware[i](handler)
		}
	}

	defer conn.Close() // 关闭流，防止文件描述符占用
	for {
		data, requestID, err := transport.ReadAndDeserialize(conn)
		if err != nil {
			// 客户端主动断开是正常行为，不打印
			if err.Error() != "EOF" && !strings.Contains(err.Error(), "EOF") {
				log.Printf("[server.go]读取客户端请求错误:%v", err)
			}
			return
		}
		res := handler(data) // 处理请求,这里直接包含中间件
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
		go s.handleRequest(conn) // 每个连接一个 goroutine
	}
}

// use中间件
func (s *RPCServer) Use(middleware middleware.Middleware) {
	// 把函数放到一个容器里
	s.middleware = append(s.middleware, middleware)
}
