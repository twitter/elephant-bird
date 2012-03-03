# Elephant Bird #

Version: 2.1.10

## About

Elephant Bird is Twitter's open source library of [LZO](http://www.github.com/kevinweil/hadoop-lzo), [Thrift](http://thrift.apache.org/), and/or [Protocol Buffer](http://code.google.com/p/protobuf)-related [Hadoop](http://hadoop.apache.org) InputFormats, OutputFormats, Writables, [Pig](http://pig.apache.org/) LoadFuncs, [Hive](http://hadoop.apache.org/hive) SerDe, [HBase](http://hadoop.apache.org/hbase) miscellanea, etc. The majority of these are in production at Twitter running over data every day.

## License

Apache licensed.

## Quickstart

1. git clone
2. ant
3. check out javadoc, etc.
4. Play with the examples: ant examples

Note: For any of the LZO-based code, make sure that the native LZO libraries are on your `java.library.path`.  Generally this is done by setting `JAVA_LIBRARY_PATH` in `pig-env.sh` or `hadoop-env.sh`.  You can also add lines like

```
PIG_OPTS=-Djava.library.path=/path/to/my/libgplcompression/dir
```

to `pig-env.sh`. See the instructions for [Hadoop-LZO](http://www.github.com/kevinweil/hadoop-lzo) for more details.

There are a few simple examples that use the input formats.  Note how the Protocol Buffer-based
formats work, and also note that the examples build file uses the custom codegen stuff.  See below for
more about that.

NOTE: This is an experimental branch for working with Pig 0.8. It may not work. Caveat emptor.

## Maven repository

Elephant Bird takes advantage of Github's raw interface and self-hosts a maven repository inside
the git repo itself. To use the maven repo, simply add `https://raw.github.com/kevinweil/elephant-bird/master/repo` as a maven repo in the system
you use to manage dependencies.

For example, with Ivy you would add the following resolver in `ivysettings.xml`:

```xml
<ibiblio name="elephant-bird-repo" m2compatible="true"
         root="https://raw.github.com/kevinweil/elephant-bird/master/repo"/>
```

And include elephant-bird as a dependency in `ivy.xml`:

```xml
<dependency org="com.twitter" name="elephant-bird" rev="${elephant-bird.version}"/>
```

## Version compatibility

1. Protocol Buffers 2.3 (not compatible with 2.4+)
2. Pig 0.8, 0.9 (not compatible with 0.7 and below)
4. Hive 0.7 (with HIVE-1616)
5. Thrift 0.5
6. Mahout 0.6
7. Cascading2 (as the API is evolving, see libraries.properties for the currently supported version)

## Protocol Buffer and Thrift compiler dependencies

Elephant Bird requires Protocol Buffer compiler version 2.3 at build time, as generated
classes are used internally. Thrift compiler version 0.5.0 is required to generate
classes used in tests. As these are native-code tools they must be installed on the build
machine (java library dependencies are pulled from maven repositories during the build).

## Contents

### Hadoop Input Formats
* JSON data
* Line-based data (TextInputFormat but for LZO; also available in deprecated 0.18 format)
* [W3C logs](http://www.w3.org/TR/WD-logfile.html)
* Serialized Protocol Buffers and Thrift messages in one of three flavors:
 * Block-based (also available in deprecated 0.18 format)
 * Block-based, into generic bytes
 * Line-based, base64 encoded

### Hadoop Writables
* Protocol Buffer and Thrift writables

### Hadoop OutputFormats
* Serialized Protocol Buffers and Thrift messages in one of two flavors
 * Block-based
 * Line-based, base64 encoded
* LZO-only (patches to make this more general would be great)

### Pig LoadFuncs
* JSON data
* Regex-based loaders
* LzoPigStorage (just what it sounds like)
* [W3C logs](http://www.w3.org/TR/WD-logfile.html)
* Serialized Protocol Buffers
 * Block-based (dynamic or via codegen, see below)
 * Line-based, base64 encoded (dynamic or via codegen, see below)
 * In SequenceFiles, using ProtobufWritableConverter
* Serialized Thrift
 * Block-based (dynamic)
 * Line-based, base64 encoded (dynamic)
 * In SequenceFiles, using ThriftWritableConverter
* SequenceFile loader
 * Includes converter interface for turning Tuples into Writables and vice versa
 * Provides implementations to convert generic Writables, Thrift, Protobufs, and other specialized classes, such as [Apache Mahout](http://mahout.apache.org/)'s [VectorWritable](http://svn.apache.org/repos/asf/mahout/trunk/core/src/main/java/org/apache/mahout/math/VectorWritable.java).

### Pig StoreFuncs
* LZO compression via LzoPigStorage
* Serialized Protobufs and Thrift
* SequenceFile Storage (with converters, as above)

### Utilities
* Counters in Pig
* Protocol Buffer utilities
* Thrift utilities
* Conversions from Protocol Buffers and Thrift messages to Pig tuples
* Conversions from Thrift to Protocol Buffer's `DynamicMessage`
* Reading and writing block-based Protocol Buffer format (see `ProtobufBlockWriter`)

## Working with Thrift and Protocol Buffers in Hadoop

We provide InputFormats, OutputFormats, Pig Load / Store functions, Hive SerDes,
and Writables for working with Thrift and Google Protocol Buffers.
We haven't written up the docs yet, but look at `ProtobufMRExample.java`, `ThriftMRExample.java`, `people_phone_number_count.pig`, `people_phone_number_count_thrift.pig` under `examples` directory for reflection-based dynamic usage.
We also provide utilities for generating Protobuf-specific Loaders, Input/Output Formats, etc, if for some reason you want to avoid
the dynamic bits.

### Protobuf Codegen?

Note: this is not strictly required for working with Protocol Buffers in Hadoop. We can do most of this dynamically.
Some people like having specific classes, though, so this functionality is available since protobuf 2.3 makes it so easy to do.

In protobuf 2.3, Google introduced the notion of a [Protocol Buffer plugin](http://code.google.com/apis/protocolbuffers/docs/reference/cpp/google.protobuf.compiler.plugin.pb.html) that
lets you hook in to their code generation elegantly, with all the parsed metadata available.  We use this in
`com.twitter.elephantbird.proto.HadoopProtoCodeGenerator` to generate code for each Protocol Buffer.  The
`HadoopProtoCodeGenerator` expects as a first argument a yml file consisting of keys and lists of classnames.  For each
Protocol Buffer file read in (say from `my_file.proto`), it looks up the basename (`my_file`) in the yml file.
If a corresponding list exists, it expects each element is a classname of a class deriving from `com.twitter.elephantbird.proto.ProtoCodeGenerator`.  These classes implement
a method to set the filename, and a method to set the generated code contents of the file.  You can add your own by creating
such a derived class and including it in the list of classnames for the Protocol Buffer file key.  That is, if you want
to apply the code generators in `com.twitter.elephantbird.proto.codegen.ProtobufWritableGenerator` and
`com.twitter.elephantbird.proto.codegen.LzoProtobufBytesToPigTupleGenerator` to every protobuf in the
file `my_file.proto`, then your config file should have a section that looks like

```
my_file:
  - com.twitter.elephantbird.proto.codegen.ProtobufWritableGenerator
  - com.twitter.elephantbird.proto.codegen.LzoProtobufBytesToPigTupleGenerator
```

There are examples in the examples subdirectory showing how to integrate this code generation into a build, both for generating Java files pre-jar and for generating other types of files from Protocol Buffer definitions post-compile (there are examples that do this to generate [Pig](http://hadoop.apache.org/pig) loaders for a set of Protocol Buffers).

## Hadoop SequenceFiles and Pig

Reading and writing Hadoop SequenceFiles with Pig is supported via classes
[SequenceFileLoader](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/load/SequenceFileLoader.java)
and
[SequenceFileStorage](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/store/SequenceFileStorage.java). These
classes make use of a
[WritableConverter](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/util/WritableConverter.java)
interface, allowing pluggable conversion of key and value instances to and from
Pig data types.

Here's a short example: Suppose you have `SequenceFile<Text, LongWritable>` data
sitting beneath path `input`. We can load that data with the following Pig
script:

```
REGISTER '/path/to/elephant-bird.jar';

%declare SEQFILE_LOADER 'com.twitter.elephantbird.pig.load.SequenceFileLoader';
%declare TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';
%declare LONG_CONVERTER 'com.twitter.elephantbird.pig.util.LongWritableConverter';

pairs = LOAD 'input' USING $SEQFILE_LOADER (
  '-c $TEXT_CONVERTER', '-c $LONG_CONVERTER'
) AS (key: chararray, value: long);
```

To store `{key: chararray, value: long}` data as `SequenceFile<Text, LongWritable>`, the following may be used:

```
%declare SEQFILE_STORAGE 'com.twitter.elephantbird.pig.store.SequenceFileStorage';

STORE pairs INTO 'output' USING $SEQFILE_STORAGE (
  '-c $TEXT_CONVERTER', '-c $LONG_CONVERTER'
);
```

For details, please see Javadocs in the following classes:
* [SequenceFileLoader](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/load/SequenceFileLoader.java)
* [SequenceFileStorage](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/store/SequenceFileStorage.java)
* [WritableConverter](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/util/WritableConverter.java)
* [GenericWritableConverter](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/util/GenericWritableConverter.java)
* [AbstractWritableConverter](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/util/AbstractWritableConverter.java)

## How To Contribute

Bug fixes, features, and documentation improvements are welcome! Please fork the project and send us a pull request on github.

Each new release since 2.1.3 has a *tag*. The latest version on master is what we are actively running on Twitter's hadoop clusters daily, over hundreds of terabytes of data.

## Contributors

Major contributors are listed below. Lots of others have helped too, thanks to all of them!
See git logs for credits.

* Kevin Weil ([@kevinweil](http://twitter.com/kevinweil))
* Dmitriy Ryaboy ([@squarecog](http://twitter.com/squarecog))
* Raghu Angadi ([@raghuangadi](http://twitter.com/raghuangadi))
* Andy Schlaikjer ([@sagemintblue](http://twitter.com/sagemintblue))
* Travis Crawford ([@tc](http://twitter.com/tc))
