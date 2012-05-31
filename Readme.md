# Elephant Bird #

## About

Elephant Bird is Twitter's open source library of [LZO](https://github.com/twitter/hadoop-lzo), [Thrift](http://thrift.apache.org/), and/or [Protocol Buffer](http://code.google.com/p/protobuf)-related [Hadoop](http://hadoop.apache.org) InputFormats, OutputFormats, Writables, [Pig](http://pig.apache.org/) LoadFuncs, [Hive](http://hadoop.apache.org/hive) SerDe, [HBase](http://hadoop.apache.org/hbase) miscellanea, etc. The majority of these are in production at Twitter running over data every day.

Join the conversation about Elephant-Bird on the [developer mailing list](https://groups.google.com/forum/?fromgroups#!forum/elephantbird-dev).

## License

Apache licensed.

## Quickstart

1. Get the code: `git clone git://github.com/kevinweil/elephant-bird.git`
1. Build the jar: `mvn package`
1. Explore what's available: `mvn javadoc:javadoc`

Note: For any of the LZO-based code, make sure that the native LZO libraries are on your `java.library.path`.  Generally this is done by setting `JAVA_LIBRARY_PATH` in `pig-env.sh` or `hadoop-env.sh`.  You can also add lines like

```
PIG_OPTS=-Djava.library.path=/path/to/my/libgplcompression/dir
```

to `pig-env.sh`. See the instructions for [Hadoop-LZO](http://www.github.com/kevinweil/hadoop-lzo) for more details.

There are a few simple examples that use the input formats. Note how the Protocol Buffer and Thrift
classes are passed to input formats through configuration.

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
5. Thrift 0.5.0, 0.6.0, 0.7.0
6. Mahout 0.6
7. Cascading2 (as the API is evolving, see libraries.properties for the currently supported version)

## Protocol Buffer and Thrift compiler dependencies

Elephant Bird requires Protocol Buffer compiler version 2.3 at build time, as generated
classes are used internally. Thrift compiler is required to generate classes used in tests.
As these are native-code tools they must be installed on the build
machine (java library dependencies are pulled from maven repositories during the build).

## Contents

### Hadoop Input and Output Formats

Elephant-Bird provides input and output formats for working with working with a variety of plaintext formats stored in LZO compressed files.

* JSON data
* Line-based data (TextInputFormat but for LZO)
* [W3C logs](http://www.w3.org/TR/WD-logfile.html)

Additionally, protocol buffers and thrift messages can be stored in a variety of file formats.

* Block-based, into generic bytes
* Line-based, base64 encoded
* SequenceFile
* RCFile

### Hadoop API wrappers

Hadoop provides two API implementations: the the old-style `org.apache.hadoop.mapred` and new-style `org.apache.hadoop.mapreduce` packages. Elephant-Bird provides wrapper classes that allow unmodified usage of `mapreduce` input and output formats in contexts where the `mapred` interface is required.

For more information, see [DeprecatedInputFormatWrapper.java](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/mapred/input/DeprecatedInputFormatWrapper.java) and [DeprecatedOutputFormatWrapper.java](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/mapred/output/DeprecatedOutputFormatWrapper.java)


### Hadoop Writables
* Elephant-Bird provides protocol buffer and thrift writables for directly working with these formats in map-reduce jobs.

### Pig Support

Loaders and storers are available for the input and output formats listed above. Additionally, pig-specific features include:

* JSON loader (including nested structures)
* Regex-based loader
* Includes converter interface for turning Tuples into Writables and vice versa
* Provides implementations to convert generic Writables, Thrift, Protobufs, and other specialized classes, such as [Apache Mahout](http://mahout.apache.org/)'s [VectorWritable](http://svn.apache.org/repos/asf/mahout/trunk/core/src/main/java/org/apache/mahout/math/VectorWritable.java).

### Hive Support

Elephant-Bird has experimental Hive support. For more information, see [How to use Elephant Bird with Hive](https://github.com/kevinweil/elephant-bird/wiki/How-to-use-Elephant-Bird-with-Hive).

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

* Kevin Weil ([@kevinweil](https://twitter.com/kevinweil))
* Dmitriy Ryaboy ([@squarecog](https://twitter.com/squarecog))
* Raghu Angadi ([@raghuangadi](https://twitter.com/raghuangadi))
* Andy Schlaikjer ([@sagemintblue](https://twitter.com/sagemintblue))
* Travis Crawford ([@tc](https://twitter.com/tc))
* Johan Oskarsson ([@skr](https://twitter.com/skr))
