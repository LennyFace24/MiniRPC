package server

import (
	"log"
	"net"
	"strings"

	"mini-rpc/internal/transport"
	"mini-rpc/internal/types"
)

func NewRPCServer() *RPCServer {
	return &RPCServer{functions: make(map[string]types.Function)}
}

// server
type RPCServer struct {
	functions map[string]types.Function
}

// 注册函数
func (s *RPCServer) RegisterFunction(function types.Function) {
	s.functions[function.Name] = function
}

// 接收客户端的函数调用请求
func (s *RPCServer) handleRequest(conn net.Conn) {
	defer conn.Close() // 关闭流，防止文件描述符占用
	for {
		data, err := transport.ReadAndDeserialize(conn)
		if err != nil {
			// 客户端主动断开是正常行为，不打印
			if err.Error() != "EOF" && !strings.Contains(err.Error(), "EOF") {
				log.Printf("[server.go]读取客户端请求错误:%v", err)
			}
			return
		}
		result, err := s.functions[data.FuncName].Call(data.Arguments...)
		if err != nil {
			log.Printf("[server.go]调用函数错误:%v", err)
			continue

		}
		// 封装res，响应结果
		res := types.ResponseData{
			Returns: result,
			Error:   "",
		}
		err = transport.SendToClient(conn, res)
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
