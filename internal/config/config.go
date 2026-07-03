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

var config *Config
var once sync.Once

func LoadConfig() *Config {
	once.Do(func() {
		// 从配置文件中加载配置
		bytes, err := os.ReadFile("config.yaml")

		if err != nil {
			log.Default().Printf("[config.go]读取配置文件错误:%v", err)
			return
		}
		// 解析
		err = yaml.Unmarshal(bytes, &config)
		if err != nil {
			log.Default().Printf("[config.go]解析配置文件错误:%v", err)
			return
		}
	})

	return config
}

func GetConfig() *Config {
	return config
}
