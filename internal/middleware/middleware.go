package middleware

import (
	"fmt"
	"log"
	"time"

	"mini-rpc/internal/types"
)

type HandlerFunc func(types.RequestData) types.ResponseData // 处理函数

// 中间件
type Middleware func(HandlerFunc) HandlerFunc

// logger 中间件
var Logger Middleware = func(next HandlerFunc) HandlerFunc {
	return func(req types.RequestData) types.ResponseData {
		log.Printf("请求:%s", req.FuncName)
		start := time.Now()
		resp := next(req)
		log.Printf("响应:%s, 耗时:%v", req.FuncName, time.Since(start))
		return resp
	}
}

var Recovery Middleware = func(next HandlerFunc) HandlerFunc {
	return func(req types.RequestData) (resp types.ResponseData) {
		defer func() {
			if r := recover(); r != nil {
				resp = types.ResponseData{
					Error: fmt.Sprintf("panic:%v", r),
				}
			}
		}()
		resp = next(req)
		return resp
	}
}
