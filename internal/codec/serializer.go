package codec

var (
	protobufCodec = &ProtobufCodec{}
)

func Serialize(data interface{}) ([]byte, error)   { return protobufCodec.Serialize(data) }
func Deserialize(data []byte, v interface{}) error { return protobufCodec.Deserialize(data, v) }
