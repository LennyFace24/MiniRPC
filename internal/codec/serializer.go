package codec

import (
	"bytes"
	"encoding/gob"
)

// 序列化
func Serialize(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(data)
	return buf.Bytes(), err
}

// 反序列化
func Deserialize(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}