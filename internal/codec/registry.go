// Package codec is the engine's OXO proto layer: it turns OXO message bytes
// into JSON-like dicts and back, so a handler never sees protobuf.
//
// It is descriptor-driven, not code-generated. The engine loads a
// FileDescriptorSet — produced by protoc and cut to only the selectors an
// agent declares — and decodes/encodes with dynamic messages. That keeps the
// engine binary message-agnostic and lets each agent ship only its own slice
// of the OXO schema.
package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// oxoProtoPackage is the proto package every OXO message lives under. A
// selector is the dotted path appended to it.
const oxoProtoPackage = "ostorlab.agent.message.proto"

// messageName is the type every OXO message file declares. Resolution is by
// convention: selector "v3.asset.ip" → message
// "ostorlab.agent.message.proto.v3.asset.ip.Message". This holds across the
// whole v3 tree — there are no exceptions.
const messageName = "Message"

// Registry resolves OXO selectors to message descriptors loaded from a
// FileDescriptorSet.
type Registry struct {
	files *protoregistry.Files
}

// NewRegistry builds a Registry from a marshaled FileDescriptorSet, as written
// by `protoc --descriptor_set_out --include_imports`. The set must be in
// dependency order (imported files before importers); protoc with
// --include_imports produces that order.
func NewRegistry(fdset []byte) (*Registry, error) {
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(fdset, &set); err != nil {
		return nil, fmt.Errorf("codec: parse descriptor set: %w", err)
	}
	var (
		files *protoregistry.Files
		err   error
	)
	files, err = protodesc.NewFiles(&set)
	if err != nil {
		return nil, fmt.Errorf("codec: build descriptor files: %w", err)
	}
	return &Registry{files: files}, nil
}

// Message resolves the message descriptor for a selector by OXO convention.
func (r *Registry) Message(selector string) (protoreflect.MessageDescriptor, error) {
	full := protoreflect.FullName(oxoProtoPackage + "." + selector + "." + messageName)
	var (
		d   protoreflect.Descriptor
		err error
	)
	d, err = r.files.FindDescriptorByName(full)
	if err != nil {
		return nil, fmt.Errorf("codec: resolve selector %q (%s): %w", selector, full, err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("codec: %s is not a message", full)
	}
	return md, nil
}

// Has reports whether the registry can resolve the selector.
func (r *Registry) Has(selector string) bool {
	var err error
	_, err = r.Message(selector)
	return err == nil
}
