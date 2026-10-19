package config

import (
	"log"
	"os"
	"sync"

	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	Server struct {
		URL  string `yaml:"url"`
		Port int    `yaml:"port"`
	} `yaml:"server"`
}

var (
	config *Config
	mu     sync.Mutex
)

func LoadConfig() *Config {
	mu.Lock()
	defer mu.Unlock()

	if config != nil {
		return config
	}

	// 从配置文件中加载配置；失败不缓存 nil，下一次调用可以重试。
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Printf("[config.go]读取配置文件错误:%v", err)
		return nil
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Printf("[config.go]解析配置文件错误:%v", err)
		return nil
	}

	config = &cfg
	return config
}

func GetConfig() *Config {
	mu.Lock()
	defer mu.Unlock()
	return config
}
