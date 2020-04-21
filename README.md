# The `go-tfdata` library

The `go-tfdata` is a Go library helping to work with tar/tgz archives and files in 
[TFRecord and tf.Example formats](https://www.tensorflow.org/tutorials/load_data/tfrecord).
It provides interfaces and their default implementations on each intermediate step between tar and TFRecord format.
Additionally it includes easy to use utilities to convert and augment data in intermediate steps.  

The library is designed with simplicity, speed and extensibility in mind. The goal is not to support multiple, complicated
communication protocols for remote data handling or complex algorithms implementations, it's rather giving ability for
users to extend it in any possible way.  

### Available Commands

`go-tfdata` provides default implementations for manipulating tar and TFRecord files. It includes:

- `FromTar(io.Reader)` - read Samples from `io.Reader` in Tar format
- `TransformSamples(transformations)` - transform each `Sample` according to provided transformations (either predeclared in `go-tfdata`
or provided by a user)
- `DefaultSampleToTFExample` - default transformation from `Sample` to `TFExample` format
- `TransformTFExamples(transformations)` - transform each `TFExample` according to provided transformations
- `ToTFRecord(io.Writer)` - write serialized TFExamples to `io.Writer` in TFRecord file format

### Available transformations and selections

`go-tfdata` provides basic Samples and TFExamples transformations and selections, which can be easily applied to the data

#### Selections

- `ByKey(key)` - selects entry which key equals to `key`
- `ByKeyValue(key, value)` - selects entry which key equals `key` and value equals `value`
- `ByPrefix(name)`, `BySuffix(name)`, `BySubstring(name)` - selects entries which key is prefix, suffix or substring of `name`
- `BySampleF(f)`, `ByExampleF(f)` - selects entries which keys are in subset returned by function `f`
- TBA...

#### Transformations

- `RenameTransformation(dest string, src []string)` - renames `src` fields into `dest` field
- TBA...

### Examples

##### Convert Tar file to TFRecord

```go
pipeline := NewPipeline().FromTar(inFile).DefaultSampleToTFExample().ToTFRecord(outFile)
pipeline.Do()
```

##### Convert Tar file to TFRecord, log every 10 TFExamples

```go
type Logger struct {
    reader TFExampleReader
    cnt    int
}

func (l *Logger) Read() (*TFExample, bool) {
    cnt++
    if cnt % 10 == 0 { log.Infof("read %d examples", cnt) }
    return l.reader.Read()
}

pipeline := NewPipeline().WithTFExampleStage(func(reader TFExampleReader) TFExampleReader {
    return &Logger{reader: reader}
}).FromTar(inFile).DefaultSampleToTFExample().ToTFRecord(outFile)

pipeline.Do()
```

##### Convert TarGz file to TFRecord, select only "image" entries from Samples

```go
pipeline := NewPipeline().TransformSamples(
    transform.ExampleSelections(selection.ByKey("image"))
).FromTarGz(inFile).DefaultSampleToTFExample().ToTFRecord(outFile)
pipeline.Do()
```

##### Convert Tar file to TFRecord, transform Samples in FAAS service

```go
type FAASClient struct { 
    reader SamplesReader
    ...
}

func (c *FAASClient) Read() (*Sample, bool) {
    sample, ok := c.reader.Read()
    if !ok { return nil, false }
    id := c.Send(sample)
    c.Receive(id, &sample)
    return sample, true
}

pipeline := NewPipeline().WithSamplesStage(func(reader SamplesReader) SamplesReader {
    return FAASClient{reader: reader} 
}).FromTar(inFile).DefaultSampleToTFExample().ToTFRecord(outFile)
pipeline.Do()
```

To see fully working implementation of some examples see `go-tfdata/tests` package.

### Internals

#### Pipeline

`pipeline` is abstraction for TAR-to-TFRecord process. `pipeline` is made of `stages`. Default pipeline implementation has 5 stages:

| Stage | Consumes | Produces | Required |  
| --- | --- | --- | --- |  
| `TarStage` | - | `SamplesReader` | Yes |  
| `SamplesStage` | `SamplesReader` | `SamplesReader` | No |  
| `Sample2ExampleStage` | `SamplesReader` | `TFExampleReader` | Yes |  
| `TFExamplesStage` | `TFExampleReader` | `TFExampleReader` | No |  
| `TFRecordStage` | `TFExampleReader` | - | No |  

With this approach, evaluation can be (but doesn't have to be) lazy, meaning that each of the stages process the data when final consumer - `TFRecordStage` -
decides to consume a TFExample

Pipeline is high-level abstraction and can be replaced, extended or limited.
For each stage, default implementation can be used (or none at all for optional stages), or custom implementation can be provided by a user via `pipeline.With[STAGE]` method

#### Readers

There exists two types of readers interfaces - `SamplesReader`, `TFExamplesReader`. Their methods:
```
TFExampleReader interface {
    Read() (ex *TFExample, ok bool)
}
```

```
SampleReader interface {
    Read() (sample *Sample, ok bool)
}
```

It's up to Reader implementation how it behaves on creation or `Read` calls. It might be executing a transformation only when
`Read` method is called (lazy) or Reader can drain internal Reader and do transformations immediately. It can as well 
prefetch part of internal Reader data. Each of approaches has it's advantages and should be considered per use-case.

#### TFExample

TFExample format is based on [TensorFlow](https://www.tensorflow.org/) [example.proto](https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/example)
files. Thanks to [Go Protobuf API v2](https://blog.golang.org/protobuf-apiv2), a structure of TFExamples in TFRecord files is determined
automatically. Learn more about [TFExample](https://www.tensorflow.org/tutorials/load_data/tfrecord#tfexample).
