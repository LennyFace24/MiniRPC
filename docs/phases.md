# mini-rpc 阶段规划

## Phase 1 — 基础设施 ✅
- types（Function, RequestData, ResponseData）
- codec（JSON → gob 序列化）
- protocol（15B 协议头：Magic/Version/MessageType/RequestID/BodyLeng）

## Phase 2 — 传输层 + 核心流程 ✅
- transport.ReadAndDeserialize / SendToClient / SendToServer
- server: Start（Accept 循环）, handleRequest（连接 goroutine + for 循环处理请求）
- client: Call（同步调用）
- pkg/rpc（对外入口）
- cmd/server、cmd/client（demo 跑通 3+5=8）

## Phase 3 — 功能完善 ✅
- 函数名不存在写回 error 给客户端
- 反射自动注册（Register(&Arith{})，不再手写闭包）
- middleware 中间件链（Logger + Recovery）

## Phase 4 — 进阶功能

### 4.1 异步调用 ✅
- CallAsync 返回 Future，Get() 阻塞等结果

### 4.2 多路复用 ✅
- 持久 conn，readLoop 后台分发
- RequestID 匹配请求和响应

### 4.3 连接池 ⏳
- 管理复用多条 TCP 连接，轮询分配请求
- 避免单连接瓶颈，提升吞吐

### 4.4 超时控制
- context.Context 透传，CallAsync 支持超时

### 4.5 Protobuf 支持
- codec 抽成接口，增加 protobuf 实现

### 4.6 服务发现存根