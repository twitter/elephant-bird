# Elephant Bird for [Apache Crunch](http://crunch.apache.org)

[Apache Crunch](http://crunch.apache.org/intro.html) is a Java library for writing, testing, and running MapReduce pipelines. One of Crunch's
goals is to make it easy to write and test pipelines that process complex records containing nested and repeated
data structures, like protocol buffers and Thrift records. This module contains support for Crunch's
[PType](http://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/types/PType.html) serialization for
Elephant Bird's `ProtobufWritable` and `ThriftWritable` classes, along with
[Source](http://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/Source.html), [Target](http://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/Target.html),
and [SourceTarget](http://crunch.apache.org/apidocs/0.8.0/org/apache/crunch/SourceTarget.html) implementations to support
Elephant Bird's `LzoProtobufBlockInputFormat`, `LzoThriftBlockInputFormat`, `LzoProtobufBlockOutputFormat`, and
`LzoThriftBlockOutputFormat`.
