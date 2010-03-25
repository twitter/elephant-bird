# Elephant Bird #

#### Twitter's library of [LZO](http://www.github.com/kevinweil/hadoop-lzo) and/or [Protocol Buffer](http://code.google.com/p/protobuf)-related [Hadoop](http://hadoop.apache.org) InputFormats, OutputFormats, Writables, [Pig](http://hadoop.apache.org/pig) LoadFuncs, [HBase](http://hadoop.apache.org/hbase) miscellanea, etc. ####

### To Use ###

1. git clone
2. ant
3. check out javadoc, etc.
4. build examples: cd examples && ant;

There are a few simple examples that use the input formats.  Note how the protocol buffer-based
formats work, and also note that the examples build file uses the custom codegen stuff.  See below for
more about that.

### Contents ###

##### LZO-based Hadoop Input Formats #####
* JSON data
* Line-based data (TextInputFormat but for LZO; also available in deprecated 0.18 format)
* [W3C logs](http://www.w3.org/TR/WD-logfile.html)
* Serialized protocol buffers in one of three flavors
    * Block-based (via codegen, see below; also available in deprecated 0.18 format)
    * Block-based, into generic bytes.
    * Line-based, base64 encoded (via codegen, see below)
    
##### LZO-based Hadoop Writables #####
* Protocol buffer writables (via codegen, see below)

##### LZO-based Hadoop OutputFormats #####
* Serialized protocol buffers in one of two flavors
    * Block-based (via codegen, see below)
    * Line-based, base64 encoded (via codegen, see below)

##### LZO-based LoadFuncs for Pig #####
* JSON data
* Regex-based loaders
* Sampling loaders for LZO
* Text loaders (TextLoader equivalent but for LZO)
* Tokenized loaders (PigStorage equivalent but for LZO)
* [W3C logs](http://www.w3.org/TR/WD-logfile.html)
* Serialized protocol buffers in one of two flavors
    * Block-based (via codegen, see below)
    * Line-based, base64 encoded (via codegen, see below)

##### LZO-based StoreFuncs for Pig #####
* Tokenized storage (PigStorage equivalent but for LZO)
* Serialized protocol buffers in one flavor
    * Line-based, base64 encoded (via codegen, see below)
    
##### Utilities #####
* Counters in Pig
* Protocol buffer utilities
* Conversions from protocol buffers to pig scripts
* Reading and writing block-based protocol buffer format (see ProtobufBlockWriter)

### Protobuf Codegen? ###

Yes. Most of the work with protobufs can be templatized via a `<M extends Message>` (where `Message`
means `com.google.protobuf.Message`) but Hadoop works mainly via reflection.  [Java type erasure](http://java.sun.com/docs/books/tutorial/java/generics/erasure.html) prevents templated types from
being instantiated properly via reflection, so our solution is to write the templatized class, and then
generate code for derived classes that do little more than instantiate the type parameter.  This causes a slight
proliferation of classes, but it only needs to be done during the build phase (don't check in generated code!)
so in practice it isn't an issue.  It turns out this can be done for Hadoop, Pig, HBase, etc, and you can easily
add your own classes to it.  The model we use here is file-based: essentially, you can configure it such that you
generate some set of derived protobuf-related classes for each protocol buffer defined in a given file.  This distribution
ships with classes

In protobuf 2.3, Google introduced the notion of a [protocol buffer plugin](http://code.google.com/apis/protocolbuffers/docs/reference/cpp/google.protobuf.compiler.plugin.pb.html) that 
lets you hook in to their code generation elegantly, with all the parsed metadata available.  We use this in 
`com.twitter.elephantbird.proto.HadoopProtoCodeGenerator` to generate code for each protocol buffer.  The 
`HadoopProtoCodeGenerator` expects as a first argument a yml file consisting of keys and lists of classnames.  For each
protocol buffer file read in (say from my__file.proto), it looks up the basename (my__file) in the yml file.  
If a corresponding list exists, it expects each element is a classname of a class deriving from `com.twitter.elephantbird.proto.ProtoCodeGenerator`.  These classes implement
a method to set the filename, and a method to set the generated code contents of the file.  You can add your own by creating
such a derived class and including it in the list of classnames for the protocol buffer file key.  That is, if you want
to apply the code generators in `com.twitter.elephantbird.proto.codegen.ProtobufWritableGenerator` and 
`com.twitter.elephantbird.proto.codegen.LzoProtobufBytesToPigTupleGenerator` to every protobuf in the
file my__file.proto, then your config file should have a section that looks like
<code><pre>
my__file:
  - com.twitter.elephantbird.proto.codegen.ProtobufWritableGenerator
  - com.twitter.elephantbird.proto.codegen.LzoProtobufBytesToPigTupleGenerator
</pre></code>

### Contributors ###

* Kevin Weil ([@kevinweil](http://twitter.com/kevinweil))
* Dmitriy Ryaboy ([@squarecog](http://twitter.com/squarecog))
* Chuang Liu ([@chuangl4](http://twitter.com/chuangl4))
* Florian Liebert ([@floliebert](http://twitter.com/floliebert))