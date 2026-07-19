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


### Phase 5 — Protobuf 支持 ✅

#### 5.1 纯 protobuf 序列化
- 删除 RequestData/ResponseData，改用 proto MessageRequest/MessageResponse
- 删除 gob 和 codec 注册中心
- 协议头 15B，传输层直接 proto.Marshal/Unmarshal

#### 5.2 代码生成（protoc-gen-go-rpc）
- 实现 protoc 插件，读取 .proto service 定义
- 生成强类型 Client（pool.CallAsync 封装）
- 生成 Server 接口 + Wrapper + RegisterCalculatorServer
- 负载均衡生效（RoundRobin 策略）

#### 5.3 Context 透传
- client 端提取 deadline/traceID → 塞进 MessageRequest.Metadata
- server 端重建 ctx → 超时检查

## 待完成阶段

### Phase 6 — TLS / 加密

#### 6.1 TLS 支持
- server 支持 `ServeTLS(cert, key)` 方法
- client 支持 `DialTLS(addr, cert)` 或 `pool.NewTLS(size, addr, tlsConfig)`
- transport 层改为 `net.Conn` 或 `tls.Conn` 均可

#### 6.2 可选的加密传输
- 非 TLS 时走明文
- 用户传入 `tls.Config` 时才走 TLS

### Phase 7 — 服务发现

#### 7.1 注册中心抽象
- 定义 `Registry` 接口：`Register(service, addr) error` + `Discover(service) ([]string, error)`
- 实现静态注册（读取配置文件）
- 实现简单的 etcd / consul 注册（可选）

#### 7.2 客户端自动发现
- client 或 pool 不再硬编码地址，改为 `Discover` 获取地址列表
- 定时刷新，感知节点上下线
- 配合负载均衡策略使用

### Phase 8 — CLI 工具（可选）
- 实现 `mini-rpc-cli` 命令行
- 支持 `call service.method arg1 arg2...` 直接调远程函数
- 类似 grpcurl
- 支持指定序列化格式（-codec json / gob）
- 支持 TLS（-tls）
