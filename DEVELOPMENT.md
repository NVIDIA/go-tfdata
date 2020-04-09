For now, just a bunch of things to remember as of April 2020:

- .proto files to generate golang source code for TensorFlow tf.Example are here:
   - https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/example
   - https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/features
   
- We want to (and we did) jump in on new Go API for Protocol Buffers: https://blog.golang.org/protobuf-apiv2
- If we want v2 we can't just `go get -u github.com/golang/protobuf/protoc-gen-go` as it's v1  
- To get v2 we have to do it from source at the moment `https://github.com/protocolbuffers/protobuf-go`.
It's possible that Go 1.14 is required so update your version (check `go version`).
Go to `cmd/protoc-gen-go`, `go install`, then run something like 
`find ./src/tensorflow/core/[example|framework] -type f -name "*.proto" -exec protoc -I ./src --go_out ./out {} \;`
where `src` is path to TensorFlow repo cloned from github

Reasons to go with v2 is that v1 is going to be deprecated sooner or later and mostly that it supports messages reflections
meaning that it's easier to unmarshal messages based on golang objects


DescriptorProto - defines dynamic schema of a message https://pkg.go.dev/google.golang.org/protobuf/types/descriptorpb?tab=doc#DescriptorProto
ToDescriptorProto - https://pkg.go.dev/google.golang.org/protobuf/reflect/protodesc?tab=doc#ToDescriptorProto
https://pkg.go.dev/google.golang.org/protobuf/types/dynamicpb?tab=doc#NewMessage