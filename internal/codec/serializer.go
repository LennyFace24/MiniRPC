package codec

import "encoding/json"

// 序列化
func Serialize(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

// 反序列化
func Deserialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
