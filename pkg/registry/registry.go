package registry

// 可定制的注册中心接口
type RegistryCenter interface {
	Register(serviceName string, address string)
	Discover(serviceName string) ([]string, bool)
}
