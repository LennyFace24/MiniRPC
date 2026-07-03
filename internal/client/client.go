package client

import (
	"log"

	"mini-rpc/internal/codec"
	"mini-rpc/internal/types"
	"mini-rpc/internal/transport"
)

type RPCClient struct {

}

func NewRPCClient() *RPCClient {
	return &RPCClient{}
}


func (c *RPCClient) Call(funcName string, args ...interface{}) ([]interface{}, error) {
	// 封装请求体
	request := types.RequestData{
		FuncName:  funcName,
		Arguments: args,
	}
	// 发送请求
	resp,err := transport.SendToServer(request)
	if err != nil {
		log.Printf("[client.go]发送请求错误:%v", err)
		return nil, err
	}
	// 读取响应
	var response types.ResponseData
	// 反序列化
	err = codec.Deserialize(resp, &response)
	if err != nil {
		log.Printf("[client.go]反序列化响应体错误:%v", err)
		return nil, err
	}
	return response.Returns, nil
}