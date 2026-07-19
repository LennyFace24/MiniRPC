package types

type ServiceDesc struct {
	ServiceName string
	Methods     map[string]*MethodDesc
}

type MethodDesc struct {
	MethodName string
	Handler    func(body []byte) ([]byte, error)
}