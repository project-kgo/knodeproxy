package proxy

import "fmt"

type rawCodec struct{}

func (rawCodec) Name() string {
	return "raw"
}

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch msg := v.(type) {
	case []byte:
		return msg, nil
	case *[]byte:
		if msg == nil {
			return nil, fmt.Errorf("raw codec: nil *[]byte")
		}
		return *msg, nil
	default:
		return nil, fmt.Errorf("raw codec: unsupported marshal type %T", v)
	}
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	switch msg := v.(type) {
	case *[]byte:
		*msg = append((*msg)[:0], data...)
		return nil
	default:
		return fmt.Errorf("raw codec: unsupported unmarshal type %T", v)
	}
}
