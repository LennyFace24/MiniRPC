package types

type Function struct {
	Name string
	Call func(body []byte) ([]byte, error)
}