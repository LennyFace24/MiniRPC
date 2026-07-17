package codec

import (
	"google.golang.org/protobuf/proto"
	myproto "mini-rpc/internal/codec/proto"
	"mini-rpc/internal/types"
)

type ProtobufCodec struct{}

func (p *ProtobufCodec) Serialize(data interface{}) ([]byte, error) {
	switch d := data.(type) {
	case types.RequestData:
		funcname := d.FuncName
		args := d.Arguments
		return proto.Marshal(&myproto.MessageRequest{
			FuncName:  funcname,
			Arguments: args,
		})
	case types.ResponseData:
		result := d.Returns
		return proto.Marshal(&myproto.MessageResponse{
			Result: result,
			Error:  d.Error,
		})
	default:
		return nil, nil
	}
}

func (p *ProtobufCodec) Deserialize(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}
