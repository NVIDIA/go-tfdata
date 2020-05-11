// Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.

// The `go-tfdata` is a Go library helping to work with tar/tgz archives and files in
// TFRecord and tf.Example formats, including converting TAR files to TFRecord files.
//
// It provides interfaces and their default implementations on each intermediate step between tar and TFRecord format.
// Additionally, it includes easy to use utilities to convert and augment data in intermediate steps.
//
// The library is designed with simplicity, speed and extensibility in mind. The goal is not to support multiple, complicated
// communication protocols for remote data handling or complex algorithms implementations, it's rather giving ability for
// users to extend it in any possible way.
package gotfdata
