# Elephant Bird #

Version: 2.1.3

#### Twitter's library of [LZO](http://www.github.com/kevinweil/hadoop-lzo), [Thrift](http://thrift.apache.org/), and/or [Protocol Buffer](http://code.google.com/p/protobuf)-related [Hadoop](http://hadoop.apache.org) InputFormats, OutputFormats, Writables, [Pig](http://pig.apache.org/) LoadFuncs, [Hive](http://hadoop.apache.org/hive) SerDe, [HBase](http://hadoop.apache.org/hbase) miscellanea, etc. The majority of these are in production at Twitter running over data every day. ####

### To Use ###

1. git clone
2. ant
3. check out javadoc, etc.
4. build examples: cd examples && ant;

Note: for any of the LZO-based code, make sure that the native LZO libraries are on your `java.library.path`.  Generally this is done by setting `JAVA_LIBRARY_PATH` in
`pig-env.sh` or `hadoop-env.sh`.  You can also add lines like
<code><pre>
PIG_OPTS=-Djava.library.path=/path/to/my/libgplcompression/dir
</pre></code>
to `pig-env.sh`. See the instructions for [Hadoop-LZO](http://www.github.com/kevinweil/hadoop-lzo) for more details.

There are a few simple examples that use the input formats.  Note how the protocol buffer-based
formats work, and also note that the examples build file uses the custom codegen stuff.  See below for
more about that.

NOTE: This is an experimental branch for working with Pig 0.8. It may not work. Caveat emptor.

### Version compatibility ###

1. Protocol Buffers 2.3 (not compatible with 2.4+)
2. Pig 0.8. 0.9 (not compatible with 0.7 and below)
4. Hive 0.7 (with HIVE-1616)
5. Thrift 0.5


#### Building Without Protocol Buffers ####

If you don't want to build elephant-bird with protobuf support or you don't have protobuf >= 2.3 available 
(fedora for instance only provides 2.2), you can have ant exclude all classes that rely on protobuf by using ant noproto target

for the default target this would be:
<code><pre>ant noproto release-jar</pre></code>

### License ###

Apache licensed.

### Contents ###

##### Hadoop Input Formats #####
* JSON data
* Line-based data (TextInputFormat but for LZO; also available in deprecated 0.18 format)
* [W3C logs](http://www.w3.org/TR/WD-logfile.html)
* Serialized protocol buffers in one of three flavors
    * Block-based (also available in deprecated 0.18 format)
    * Block-based, into generic bytes.
    * Line-based, base64 encoded
* Same as protocol buffers, but Thrift.

    
##### Hadoop Writables #####
* Protocol buffer and Thrift writables 

##### Hadoop OutputFormats #####
* Serialized protocol buffers and Thrift messages in one of two flavors
    * Block-based
    * Line-based, base64 encoded
* LZO-only (patches to make this more general would be great)


##### LoadFuncs for Pig #####
* JSON data
* Regex-based loaders
* LzoPigStorage (just what it sounds like)
* [W3C logs](http://www.w3.org/TR/WD-logfile.html)
* Serialized protocol buffers
    * Block-based (dynamic or via codegen, see below)
    * Line-based, base64 encoded (dynamic or via codegen, see below)
    * In SequenceFiles, using ProtobufWritableConverter
* Serialized Thrift
    * Block-based (dynamic)
    * Line-based, base64 encoded (dynamic)
    * In SequenceFiles, using ThriftWritableConverter
* SequenceFile Loaders
    * Has converter interface for turning Tuples into Writable
    * provides implementations to convert generic Writables, Thrift, Protobufs

##### LZO-based StoreFuncs for Pig #####
* LzoPigStorage
* Serialized Protobufs and Thrift 
* SequenceFile Storage (with converters, as above)
    
##### Utilities #####
* Counters in Pig
* Protocol buffer utilities
* Thrift utilities
* Conversions from protocol buffers and Thrift messages to pig tuples
* Conversions from Thrift to PB's DynamicMessage
* Reading and writing block-based protocol buffer format (see ProtobufBlockWriter)

### Working with Thrift and Protocol Buffers in Hadoop ###

We provide InputFormats, OutputFormats, Pig Load / Store functions, Hive SerDes,
and Writables for working with Thrift and Google Protocol Buffers. 
We haven't written up the docs yet, but look at `ProtobufMRExample.java`, `ThriftMRExample.java`, `people_phone_number_count.pig`, `people_phone_number_count_thrift.pig` under `examples` directory for reflection-based dynamic usage.
We also provide utilities for generating Protobuf-specific Loaders, Input/Output Formats, etc, if for some reason you want to avoid
the dynamic bits.

### Protobuf Codegen? ###

Note: this is not strictly required for working with Protocol Buffers in Hadoop. We can do most of this dynamically.
Some people like having specific classes, though, so this functionality is available since protobuf 2.3 makes it so easy to do.

In protobuf 2.3, Google introduced the notion of a [protocol buffer plugin](http://code.google.com/apis/protocolbuffers/docs/reference/cpp/google.protobuf.compiler.plugin.pb.html) that 
lets you hook in to their code generation elegantly, with all the parsed metadata available.  We use this in 
`com.twitter.elephantbird.proto.HadoopProtoCodeGenerator` to generate code for each protocol buffer.  The 
`HadoopProtoCodeGenerator` expects as a first argument a yml file consisting of keys and lists of classnames.  For each
protocol buffer file read in (say from `my_file.proto`), it looks up the basename (`my_file`) in the yml file.  
If a corresponding list exists, it expects each element is a classname of a class deriving from `com.twitter.elephantbird.proto.ProtoCodeGenerator`.  These classes implement
a method to set the filename, and a method to set the generated code contents of the file.  You can add your own by creating
such a derived class and including it in the list of classnames for the protocol buffer file key.  That is, if you want
to apply the code generators in `com.twitter.elephantbird.proto.codegen.ProtobufWritableGenerator` and 
`com.twitter.elephantbird.proto.codegen.LzoProtobufBytesToPigTupleGenerator` to every protobuf in the
file `my_file.proto`, then your config file should have a section that looks like
<code><pre>
my_file:
  - com.twitter.elephantbird.proto.codegen.ProtobufWritableGenerator
  - com.twitter.elephantbird.proto.codegen.LzoProtobufBytesToPigTupleGenerator
</pre></code>

There are examples in the examples subdirectory showing how to integrate this code generation into a build, both for generating Java files pre-jar and for generating other types of files from protocol buffer definitions post-compile (there are examples that do this to generate [Pig](http://hadoop.apache.org/pig) loaders for a set of protocol buffers).  

### SequenceFiles and Pig ###

For details of how the Pig integration with SequenceFiles works, please see javadocs for the following classes:
* ([SequenceFileLoader](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/load/SequenceFileLoader.java))
* ([SequenceFileStorage](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/store/SequenceFileStorage.java))
* ([GenericWritableConverter](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/util/GenericWritableConverter.java))
* ([AbstractWritableConverter](https://github.com/kevinweil/elephant-bird/blob/master/src/java/com/twitter/elephantbird/pig/util/AbstractWritableConverter.java))


### How To Contribute ###

Bug fixes, features, and documentation improvements are welcome!

Please fork the *eb-dev* branch and send us a pull request on github.

We merge eb-dev into master every few weeks. The latest version on master is what we are actively running on Twitter's hadoop clusters daily, over hundreds of terabytes of data.


### Contributors ###

Major contributors are listed below. Lots of others have helped too, thanks to all of them!
See git logs for credits.

* Kevin Weil ([@kevinweil](http://twitter.com/kevinweil))
* Dmitriy Ryaboy ([@squarecog](http://twitter.com/squarecog))
* Raghu Angadi ([@raghuangadi](http://twitter.com/raghuangadi))
* Andy Schlaikjer ([@sagemintblue])(http://twitter.com/sagemintblue))
