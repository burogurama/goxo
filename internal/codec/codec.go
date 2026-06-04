// Dict↔wire conversion: a Codec runs a selector's descriptor through dynamicpb
// and protojson to turn OXO message bytes into a dict and back.
package codec

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Codec decodes OXO message bytes to dicts and encodes dicts back to bytes,
// using the descriptors in a Registry. The dict is the engine↔handler payload
// representation; it follows protobuf's canonical JSON shape — field names are
// the proto (snake_case) names, 64-bit ints are strings, bytes are
// base64-encoded, enums are their value names. That shape survives a plain
// JSON parser in any language without fidelity loss.
type Codec struct {
	reg       *Registry
	marshal   protojson.MarshalOptions
	unmarshal protojson.UnmarshalOptions
}

// New builds a Codec over the registry.
func New(reg *Registry) *Codec {
	var resolver *dynamicpb.Types = dynamicpb.NewTypes(reg.files)
	return &Codec{
		reg: reg,
		marshal: protojson.MarshalOptions{
			UseProtoNames:   true,
			EmitUnpopulated: false,
			Resolver:        resolver,
		},
		unmarshal: protojson.UnmarshalOptions{
			Resolver: resolver,
		},
	}
}

// Decode parses wire bytes for the selector into a dict.
func (c *Codec) Decode(selector string, wire []byte) (map[string]any, error) {
	var (
		md  protoreflect.MessageDescriptor
		err error
	)
	md, err = c.reg.Message(selector)
	if err != nil {
		return nil, err
	}
	var msg *dynamicpb.Message = dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(wire, msg); err != nil {
		return nil, fmt.Errorf("codec: decode %s: %w", selector, err)
	}
	var js []byte
	js, err = c.marshal.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("codec: marshal %s to json: %w", selector, err)
	}
	dict := map[string]any{}
	if err := json.Unmarshal(js, &dict); err != nil {
		return nil, fmt.Errorf("codec: json to dict %s: %w", selector, err)
	}
	return dict, nil
}

// Encode turns a dict into wire bytes for the selector. The dict must follow
// the canonical JSON shape Decode produces (base64 for bytes, etc.).
func (c *Codec) Encode(selector string, dict map[string]any) ([]byte, error) {
	var (
		md  protoreflect.MessageDescriptor
		err error
	)
	md, err = c.reg.Message(selector)
	if err != nil {
		return nil, err
	}
	var js []byte
	js, err = json.Marshal(dict)
	if err != nil {
		return nil, fmt.Errorf("codec: dict to json %s: %w", selector, err)
	}
	var msg *dynamicpb.Message = dynamicpb.NewMessage(md)
	if err := c.unmarshal.Unmarshal(js, msg); err != nil {
		return nil, fmt.Errorf("codec: unmarshal json to %s: %w", selector, err)
	}
	var wire []byte
	wire, err = proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("codec: encode %s: %w", selector, err)
	}
	return wire, nil
}
