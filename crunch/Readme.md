# Elephant Bird for [Apache Crunch](https://crunch.apache.org)

[Apache Crunch](https://crunch.apache.org/intro.html) is a Java library for writing, testing, and running MapReduce pipelines. One of Crunch's
goals is to make it easy to write and test pipelines that process complex records containing nested and repeated
data structures, like protocol buffers and Thrift records. This module contains support for Crunch's
[PType](https://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/types/PType.html) serialization for
Elephant Bird's `ProtobufWritable` and `ThriftWritable` classes, along with
[Source](https://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/Source.html), [Target](https://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/Target.html),
and [SourceTarget](https://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/SourceTarget.html) implementations to support
Elephant Bird's `LzoProtobufBlockInputFormat`, `LzoThriftBlockInputFormat`, `LzoProtobufBlockOutputFormat`, and
`LzoThriftBlockOutputFormat`.
