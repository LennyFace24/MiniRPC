package middleware

import (
	"context"
	"fmt"
	"log"
	"time"

	framepb "mini-rpc/internal/codec/proto/framepb"
)

type HandlerFunc func(context.Context, *framepb.MessageRequest) *framepb.MessageResponse

type Middleware func(HandlerFunc) HandlerFunc

var Logger Middleware = func(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, req *framepb.MessageRequest) *framepb.MessageResponse {
		log.Printf("请求:%s", req.FuncName)
		start := time.Now()
		resp := next(ctx, req)
		log.Printf("响应:%s, 耗时:%v", req.FuncName, time.Since(start))
		return resp
	}
}

var Recovery Middleware = func(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, req *framepb.MessageRequest) (resp *framepb.MessageResponse) {
		defer func() {
			if r := recover(); r != nil {
				resp = &framepb.MessageResponse{
					Error: fmt.Sprintf("panic:%v", r),
				}
			}
		}()
		resp = next(ctx, req)
		return resp
	}
}
