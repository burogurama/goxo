// Package settings parses the OXO AgentInstanceSettings the runtime serializes
// to /tmp/settings.binproto and hands the engine the fields it needs to connect
// to the bus and run a handler. Like internal/codec it is descriptor-driven:
// the schema lives in an embedded FileDescriptorSet and decoding goes through
// dynamicpb, so the engine carries no generated proto code.
package settings

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

//go:generate protoc --proto_path=. --descriptor_set_out=agent_instance_settings.fdset agent_instance_settings.proto

//go:embed agent_instance_settings.fdset
var fdset []byte

// DefaultPath is where the OXO runtime writes the settings proto.
const DefaultPath = "/tmp/settings.binproto"

// settingsMessage is the fully qualified name of the settings message in the
// embedded descriptor set.
const settingsMessage = "ostorlab.runtimes.proto.AgentInstanceSettings"

// Settings is the slice of AgentInstanceSettings the engine consumes. Absent
// proto fields decode to their zero value, matching OXO's proto2 defaults; the
// engine applies its own operational defaults on top.
type Settings struct {
	Key                   string
	BusURL                string
	BusExchangeTopic      string
	Args                  []Arg
	RedisURL              string
	HealthcheckHost       string
	HealthcheckPort       uint32
	CyclicProcessingLimit uint32
	DepthProcessingLimit  uint32
	AcceptedAgents        []string
	InSelectors           []string
	ServiceName           string
}

// Arg is one agent argument as it travels on the wire: a name, an OXO type tag,
// and the still-encoded value bytes. Decode turns the bytes into a Go value
// according to the type tag.
type Arg struct {
	Name  string
	Type  string
	Value []byte
}

// Load reads and parses the settings proto at path.
func Load(path string) (*Settings, error) {
	var (
		data []byte
		err  error
	)
	data, err = os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("settings: read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse decodes binary AgentInstanceSettings bytes into Settings.
func Parse(data []byte) (*Settings, error) {
	var (
		md  protoreflect.MessageDescriptor
		err error
	)
	md, err = messageDescriptor()
	if err != nil {
		return nil, err
	}
	var msg *dynamicpb.Message = dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("settings: parse proto: %w", err)
	}
	return &Settings{
		Key:                   getString(msg, "key"),
		BusURL:                getString(msg, "bus_url"),
		BusExchangeTopic:      getString(msg, "bus_exchange_topic"),
		Args:                  getArgs(msg),
		RedisURL:              getString(msg, "redis_url"),
		HealthcheckHost:       getString(msg, "healthcheck_host"),
		HealthcheckPort:       getUint32(msg, "healthcheck_port"),
		CyclicProcessingLimit: getUint32(msg, "cyclic_processing_limit"),
		DepthProcessingLimit:  getUint32(msg, "depth_processing_limit"),
		AcceptedAgents:        getStrings(msg, "accepted_agents"),
		InSelectors:           getStrings(msg, "in_selectors"),
		ServiceName:           getString(msg, "service_name"),
	}, nil
}

// Decode returns the argument value in the engine's dict representation: a
// "binary" arg yields its raw bytes; any other type is JSON-decoded. This
// mirrors the OXO agent's args property.
func (a Arg) Decode() (any, error) {
	if a.Type == "binary" {
		return a.Value, nil
	}
	var v any
	if err := json.Unmarshal(a.Value, &v); err != nil {
		return nil, fmt.Errorf("settings: decode arg %q: %w", a.Name, err)
	}
	return v, nil
}

var (
	descOnce sync.Once
	descMD   protoreflect.MessageDescriptor
	descErr  error
)

// messageDescriptor resolves the settings message descriptor from the embedded
// descriptor set once and caches it.
func messageDescriptor() (protoreflect.MessageDescriptor, error) {
	descOnce.Do(func() {
		var set descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(fdset, &set); err != nil {
			descErr = fmt.Errorf("settings: parse descriptor set: %w", err)
			return
		}
		var files *protoregistry.Files
		files, descErr = protodesc.NewFiles(&set)
		if descErr != nil {
			descErr = fmt.Errorf("settings: build descriptor files: %w", descErr)
			return
		}
		var d protoreflect.Descriptor
		d, descErr = files.FindDescriptorByName(settingsMessage)
		if descErr != nil {
			descErr = fmt.Errorf("settings: resolve %s: %w", settingsMessage, descErr)
			return
		}
		md, ok := d.(protoreflect.MessageDescriptor)
		if !ok {
			descErr = fmt.Errorf("settings: %s is not a message", settingsMessage)
			return
		}
		descMD = md
	})
	return descMD, descErr
}

// getString reads a scalar string field, returning "" when unset.
func getString(m protoreflect.Message, name string) string {
	return m.Get(field(m, name)).String()
}

// getUint32 reads a scalar uint32 field, returning 0 when unset.
func getUint32(m protoreflect.Message, name string) uint32 {
	return uint32(m.Get(field(m, name)).Uint())
}

// getStrings reads a repeated string field, returning nil when empty.
func getStrings(m protoreflect.Message, name string) []string {
	var list protoreflect.List = m.Get(field(m, name)).List()
	if list.Len() == 0 {
		return nil
	}
	out := make([]string, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		out = append(out, list.Get(i).String())
	}
	return out
}

// getArgs reads the repeated args field into Arg values, returning nil when
// empty.
func getArgs(m protoreflect.Message) []Arg {
	var list protoreflect.List = m.Get(field(m, "args")).List()
	if list.Len() == 0 {
		return nil
	}
	out := make([]Arg, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		var am protoreflect.Message = list.Get(i).Message()
		out = append(out, Arg{
			Name:  getString(am, "name"),
			Type:  getString(am, "type"),
			Value: getBytes(am, "value"),
		})
	}
	return out
}

// getBytes reads a scalar bytes field, returning empty when unset.
func getBytes(m protoreflect.Message, name string) []byte {
	return m.Get(field(m, name)).Bytes()
}

// field resolves a field descriptor by name on the message's descriptor.
func field(m protoreflect.Message, name string) protoreflect.FieldDescriptor {
	return m.Descriptor().Fields().ByName(protoreflect.Name(name))
}
