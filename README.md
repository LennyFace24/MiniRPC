# MiniRPC

一个使用 Go 编写的轻量级 RPC 框架，基于 TCP 长连接、Protocol Buffers 和自定义二进制协议实现远程方法调用。

MiniRPC 提供客户端、服务端、连接池、异步调用、请求多路复用、负载均衡、Context deadline 透传、中间件以及 `protoc` 代码生成插件，适合学习 RPC 框架的核心原理，也可以作为轻量级内部服务通信的基础。

## 特性

- 基于 TCP 长连接，支持连接复用
- 支持请求多路复用和并发异步调用
- 使用 Protocol Buffers 序列化请求与响应
- 支持 `Future` 异步结果获取
- 支持连接池
- 支持多种负载均衡策略：
  - Round Robin
  - Least Connections
  - Random
- 支持 `context.Context` deadline 透传
- 内置日志和 panic 恢复中间件
- 支持自定义中间件
- 支持服务端 TLS
- 提供可扩展的服务注册与发现接口
- 提供 `protoc-gen-go-rpc` 代码生成插件
- 支持自动生成强类型 Client、Server 接口和服务注册函数

## 技术栈

- Go 1.25+
- TCP
- Protocol Buffers
- `google.golang.org/protobuf`
- `gopkg.in/yaml.v2`

## 项目结构

```text
.
├── cmd/
│   ├── client/              # 客户端示例
│   ├── server/              # 服务端示例
│   └── protoc-gen-go-rpc/   # RPC 代码生成插件
├── internal/
│   ├── client/              # RPC 客户端、Future、连接池和负载均衡
│   ├── codec/proto/         # Protobuf 定义和生成代码
│   ├── config/              # YAML 配置加载
│   ├── middleware/          # 日志、异常恢复等中间件
│   ├── protocol/            # RPC 协议头定义
│   ├── server/              # RPC 服务注册和请求处理
│   ├── transport/            # TCP 数据读写和 Protobuf 编解码
│   └── types/               # 服务和方法描述
├── pkg/
│   ├── registry/             # 服务注册与发现接口
│   └── rpc/                  # 对外暴露的 RPC 创建入口
├── config.yaml              # 默认服务端配置
├── address.json             # 示例服务地址列表
├── docs/                    # 开发规划和设计文档
├── go.mod
└── go.sum
```

## 协议格式

MiniRPC 使用固定长度为 15 字节的协议头：

| 字段 | 长度 | 说明 |
| --- | ---: | --- |
| Magic | 1 字节 | 固定值 `0xCC` |
| Version | 1 字节 | 当前版本为 `0x01` |
| MessageType | 1 字节 | 当前值为 `0x01` |
| RequestID | 8 字节 | 用于匹配请求和响应 |
| BodyLen | 4 字节 | Protobuf 消息体长度 |

协议头后跟随 Protobuf 编码的请求或响应数据。

## 快速开始

### 环境要求

- Go 1.25 或更高版本
- 如需重新生成 RPC 代码，还需要安装：
  - `protoc`
  - `protoc-gen-go`

### 获取项目

```bash
git clone https://github.com/LennyFace24/MiniRPC.git
cd MiniRPC
go mod download
```

### 启动服务端

```bash
go run ./cmd/server
```

服务端默认监听 `:8080`，并注册 `Calculator` 服务。

### 启动客户端

另开一个终端执行：

```bash
go run ./cmd/client
```

预期输出：

```text
[cmd/client]计算结果: 8
```

客户端会调用：

```text
Calculator.Add(3, 5)
```

### 运行测试

```bash
go test ./...
```

运行 benchmark：

```bash
go test -bench=. ./...
```

## 使用示例

### 定义 Protobuf 服务

```proto
syntax = "proto3";

package calc;

option go_package = "mini-rpc/internal/codec/proto/calc";

service Calculator {
  rpc Add(CalcReq) returns (CalcRsp);
}

message CalcReq {
  int32 a = 1;
  int32 b = 2;
}

message CalcRsp {
  int32 result = 1;
}
```

### 实现服务端

```go
package main

import (
	"context"
	"log"
	"net"

	calc "mini-rpc/internal/codec/proto/calc"
	"mini-rpc/internal/middleware"
	"mini-rpc/pkg/rpc"
)

type CalculatorServer struct{}

func (s *CalculatorServer) Add(
	ctx context.Context,
	req *calc.CalcReq,
) (*calc.CalcRsp, error) {
	return &calc.CalcRsp{
		Result: req.A + req.B,
	}, nil
}

func main() {
	server := rpc.NewServer()

	calc.RegisterCalculatorServer(server, &CalculatorServer{})

	server.Use(middleware.Logger)
	server.Use(middleware.Recovery)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	server.Start(listener)
}
```

### 调用服务

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	calc "mini-rpc/internal/codec/proto/calc"
)

func main() {
	client := calc.NewCalculatorClient("localhost:8080")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	rsp, err := client.Add(ctx, &calc.CalcReq{
		A: 3,
		B: 5,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(rsp.Result)
}
```

输出：

```text
8
```

## 使用连接池

```go
pool := rpc.NewPool(4, "localhost:8080")
```

通过连接池发起调用：

```go
body, err := proto.Marshal(&calc.CalcReq{
	A: 3,
	B: 5,
})
if err != nil {
	log.Fatal(err)
}

future, err := pool.CallAsync(
	context.Background(),
	"Calculator.Add",
	client.RoundRobin,
	body,
)
if err != nil {
	log.Fatal(err)
}

result, err := future.Get(context.Background())
if err != nil {
	log.Fatal(err)
}

var rsp calc.CalcRsp
if err := proto.Unmarshal(result, &rsp); err != nil {
	log.Fatal(err)
}

fmt.Println(rsp.Result)
```

可用的负载均衡策略：

```go
client.RoundRobin
client.LeastConnections
client.Random
```

## Protobuf RPC 代码生成

构建 RPC 代码生成插件：

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
go build -o protoc-gen-go-rpc ./cmd/protoc-gen-go-rpc
```

确保以下命令位于 `PATH` 中：

```text
protoc
protoc-gen-go
protoc-gen-go-rpc
```

然后执行：

```bash
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-rpc_out=. \
  --go-rpc_opt=paths=source_relative \
  path/to/service.proto
```

插件会为每个 Protobuf service 生成：

- 强类型 Client
- Server 接口
- Server Wrapper
- `Register<Service>Server` 注册函数

生成的客户端调用会自动完成：

1. Protobuf 序列化
2. RPC 请求发送
3. Future 结果等待
4. Protobuf 响应反序列化

## 配置

如果客户端没有显式指定地址，MiniRPC 会读取根目录下的 `config.yaml`：

```yaml
server:
  url: "localhost"
  port: 8080
```

客户端也可以直接指定地址：

```go
client := client.NewRPCClientWithAddr("localhost:8080")
```

示例服务地址列表位于 `address.json`：

```json
[
  "localhost:8080"
]
```

## 服务注册与发现

项目提供了可扩展的注册中心接口：

```go
type RegistryCenter interface {
	Register(serviceName string, address string)
	Discover(serviceName string) ([]string, bool)
}
```

使用自定义注册中心创建连接池：

```go
pool := rpc.NewPoolWithRegistry(1, registry)
```

当前仓库主要提供接口和静态地址示例，后续可以接入 etcd、Consul 等服务发现组件。

## 中间件

MiniRPC 内置两个中间件：

### 日志中间件

```go
server.Use(middleware.Logger)
```

记录请求名称、响应名称和执行耗时。

### Panic 恢复中间件

```go
server.Use(middleware.Recovery)
```

捕获服务处理过程中的 panic，并将其转换为 RPC 错误响应。

也可以根据需要扩展自定义中间件。

## TLS

服务端支持通过 TLS 启动监听：

```go
server := rpc.NewServer()

calc.RegisterCalculatorServer(
	server,
	&CalculatorServer{},
)

server.ServeTLS(
	":8443",
	"server.crt",
	"server.key",
)
```

客户端连接池支持传入 TLS 配置：

```go
pool := client.NewPoolTLS(
	4,
	"localhost:8443",
	tlsConfig,
)
```

## Context 超时

客户端调用时可以使用 `context.WithTimeout` 或 `context.WithDeadline`：

```go
ctx, cancel := context.WithTimeout(
	context.Background(),
	time.Second,
)
defer cancel()

rsp, err := client.Add(ctx, req)
```

客户端会将 deadline 写入 RPC metadata，服务端解析后进行超时检查。

## 当前限制

- 服务发现目前只有抽象接口和静态地址示例。
- 尚未内置 etcd 或 Consul 实现。
- CLI 调用工具仍处于规划阶段。
- 生成代码中的部分内部包路径更适合在当前模块中使用。
- 项目当前未声明开源许可证。

更多开发阶段和后续规划请参考：

- [`docs/phases.md`](docs/phases.md)
- [`docs/roadmap.md`](docs/roadmap.md)
- [`docs/process.md`](docs/process.md)

## 开发路线

计划中的功能包括：

- 完善 TLS 客户端配置
- 接入 etcd / Consul 服务发现
- 支持动态节点刷新
- 开发类似 `grpcurl` 的 CLI 工具
- 支持更多传输和调用配置
- 完善跨模块 Context 透传

## License
MIT
