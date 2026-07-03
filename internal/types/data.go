package types

// 函数统一返回规范
type Function struct {
	Name      string
	Arguments []interface{}
	Returns   []interface{}
	Call      func(args ...interface{}) ([]interface{}, error)
}

type RequestData struct {
	FuncName  string
	Arguments []interface{}
}

type ResponseData struct {
	Returns []interface{}
	Error   string
}
