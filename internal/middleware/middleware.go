package middleware

import (
	"fmt"
	"log"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
)

type HandlerFunc func(*framepb.MessageRequest) *framepb.MessageResponse

type Middleware func(HandlerFunc) HandlerFunc

var Logger Middleware = func(next HandlerFunc) HandlerFunc {
	return func(req *framepb.MessageRequest) *framepb.MessageResponse {
		log.Printf("请求:%s", req.FuncName)
		start := time.Now()
		resp := next(req)
		log.Printf("响应:%s, 耗时:%v", req.FuncName, time.Since(start))
		return resp
	}
}

var Recovery Middleware = func(next HandlerFunc) HandlerFunc {
	return func(req *framepb.MessageRequest) (resp *framepb.MessageResponse) {
		defer func() {
			if r := recover(); r != nil {
				resp = &framepb.MessageResponse{
					Error: fmt.Sprintf("panic:%v", r),
				}
			}
		}()
		resp = next(req)
		return resp
	}
}