# mini-rpc 开发规划

## 已完成阶段

### Phase 1 — 基础设施 ✅
- types（Function, RequestData, ResponseData）
- codec（JSON → gob 序列化）
- protocol（15B 协议头）

### Phase 2 — 传输层 + 核心流程 ✅
- transport: ReadAndDeserialize / SendToClient / SendToServer
- server: Start, handleRequest
- client: Call, CallAsync, Call
- pkg/rpc: 对外入口
- cmd: server / client demo

### Phase 3 — 功能完善 ✅
- 函数名不存在写回 error
- 反射自动注册
- middleware 中间件链（Logger + Recovery）

### Phase 4 — 进阶功能 ✅
- 4.1 异步调用 Future / Get
- 4.2 多路复用（持久 conn + readLoop + pending + RequestID）
- 4.3 连接池 + 负载均衡（RoundRobin / LeastConnections / Random）
- 4.4 超时控制（Get 默认 10s 超时）

---

## 待完成阶段

### Phase 5 — 可扩展序列化

#### 5.1 Codec 接口抽离
- 定义 `Codec` 接口：`Serialize(v interface{}) ([]byte, error)` + `Deserialize(data []byte, v interface{}) error`
- 现行 gob 实现改为 `GobCodec`
- 所有 `codec.Serialize` / `codec.Deserialize` 调用改为全局 `DefaultCodec` 或传参
- 用户可调 `codec.SetDefault(gobCodec)` 或 `codec.SetDefault(jsonCodec)`

#### 5.2 JSON Codec 实现
- 实现 `JSONCodec`
- 注意 JSON 反序列化数字类型问题（int → float64），可用 `json.UseNumber` 或 `map[string]interface{}` 做类型映射

#### 5.3 Protobuf 支持
- 安装 protoc + protoc-gen-go
- 在 `internal/codec` 下定义 `proto/demo.proto`：Request / Response 消息
- 实现 `ProtobufCodec`
- 与 gob / JSON 切换测试

### Phase 6 — Context 透传

#### 6.1 RequestData 加 Context
- RequestData 加 `Context map[string]string` 或 `Metadata map[string]string` 字段
- client 端将 context 中的 timeout / traceID 等序列化进请求
- server 端解析后重建 context

#### 6.2 超时从 client 传到 server
- client 的超时通过 metadata 传给 server
- server 用 `context.WithTimeout` 限制执行时间
- 超时时 server 返回超时 error，不继续执行

### Phase 7 — TLS / 加密

#### 7.1 TLS 支持
- server 支持 `ServeTLS(cert, key)` 方法
- client 支持 `DialTLS(addr, cert)` 或 `pool.NewTLS(size, addr, tlsConfig)`
- transport 层改为 `net.Conn` 或 `tls.Conn` 均可

#### 7.2 可选的加密传输
- 非 TLS 时走明文
- 用户传入 `tls.Config` 时才走 TLS

### Phase 8 — 服务发现

#### 8.1 注册中心抽象
- 定义 `Registry` 接口：`Register(service, addr) error` + `Discover(service) ([]string, error)`
- 实现静态注册（读取配置文件）
- 实现简单的 etcd / consul 注册（可选）

#### 8.2 客户端自动发现
- client 或 pool 不再硬编码地址，改为 `Discover` 获取地址列表
- 定时刷新，感知节点上下线
- 配合负载均衡策略使用

### Phase 9 — CLI 工具（可选）

- 实现 `mini-rpc-cli` 命令行
- 支持 `call service.method arg1 arg2...` 直接调远程函数
- 类似 grpcurl
- 支持指定序列化格式（-codec json / gob）
- 支持 TLS（-tls）
