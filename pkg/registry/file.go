package registry

import (
	"encoding/json"
	"os"
)

// FileRegistry 从 JSON 文件读取服务地址。
// 文件格式：
//
//	{"Calculator": ["localhost:8080", "localhost:8081"]}
//
// 它只负责发现，Register 是 no-op；适合静态配置和简单 demo。
type FileRegistry struct {
	path string
}

func NewFileRegistry(path string) *FileRegistry {
	return &FileRegistry{path: path}
}

func (r *FileRegistry) Register(serviceName string, address string) {}

func (r *FileRegistry) Discover(serviceName string) ([]string, bool) {
	data, err := os.ReadFile(r.path)
	if err != nil {
		return nil, false
	}

	var services map[string][]string
	if err := json.Unmarshal(data, &services); err != nil {
		return nil, false
	}

	addrs, ok := services[serviceName]
	if !ok {
		return nil, false
	}
	return append([]string(nil), addrs...), true
}
