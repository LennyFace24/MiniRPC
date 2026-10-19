package types

import "context"

type ServiceDesc struct {
	ServiceName string
	Methods     map[string]*MethodDesc
}

type MethodDesc struct {
	MethodName string
	Handler    func(ctx context.Context, body []byte) ([]byte, error)
}
