# Elephant Bird #

### Version 2.1.10 ###

ISSUE 156. Fix a classname bug introduced in the recent refactor. (Travis Crawford)

### Version 2.1.9 ###

ISSUE 153. Update cascading2 version and classes with changes already in science. (Travis Crawford)

ISSUE 152. Removes requirement that input Vector data match user specified -dense, -sparse options (Andy Schlaikjer)

ISSUE 148. TestInvoker#testSpeed() may fail on slow machines (Michael Noll)

ISSUE 147. Refactor JSON loaders so an InputFormat can be used & update test. (Travis Crawford)

ISSUE 142. Don't force thrift in classpath unless necessary. (Raghu Angadi)

ISSUE 141. Cleans up top level Readme.md, adds more info on SequenceFile IO (Andy Schlaikjer)

ISSUE 140. SeqFileLoader and SeqFileStorage should resolve using PigContext (Dmitriy Ryaboy)

ISSUE 139. Adds Mahout VectorWritableConverter util from Twadoop (Andy Schlaikjer)

ISSUE 138. Please bump to lastest cascading2 build (slightly incompatible with wip-176). (Oscar Boykin)

### Version 2.1.8 ###

ISSUE 137. Fixes the namespace in cascading2 protobufs (Oscar Boykin)

ISSUE 135. port index file handling from LzoInputFormat to its Deprecated sibling. (Raghu Angadi)

### Version 2.1.7 ###

ISSUE 133. Downgrade cascading2 dependency to more stable version (Travis Crawford)

### Version 2.1.6 ###

ISSUE 129. Add "install-local" ant target (Travis Crawford)

ISSUE 127. Lzo text outputformat heirarchy (Raghu Angadi)

ISSUE 126. Add protobuf-java-2.3.0.jar back as a checked-in library (Travis Crawford)

ISSUE 125. Deprecated lzo output format update. (Raghu Angadi)

ISSUE 123. Fix maven repo documentation (Travis Crawford)

NO ISSUE. Upgrade hadoop-lzo to 0.4.15 (Raghu Angadi)

### Version 2.1.5 ###

ISSUE 122. Add maven repository support (Travis Crawford)

ISSUE 121. Add Ivy & POM file generation to Elephant Bird build (Travis Crawford)

ISSUE 120. Build improvements  (Travis Crawford)

ISSUE 119. Fix a regression from 2.1.4 related to the multi-loader (Raghu Angadi)

### Version 2.1.4 ###

ISSUE 116. Use MultiInputFormat for Thrift and Protobuf Pig loaders (Raghu Angadi)

### Version 2.1.3 ###

ISSUE 117. Add Cascading2 taps (Dmitriy Ryaboy)

ISSUE 115. Use guava-11.0 (Alex Levenson)

ISSUE 112. Fix UDFContext handing in SequenceFileStorage (Ted Dunning)

### Version 2.1.2 ###

ISSUE 105. Fix ThriftToPig for Pig9 and nested containers (Jonathan Coveney)

ISSUE 97.  MultiFormatLoader and MultiInputFormat (Raghu Angadi)

ISSUE 92.  Projection support for Protobuf and Thrift loaders (Raghu Angadi)

ISSUE 102. Handle null returned from JSON parser (Michael G. Noll)

### Version 2.1.1 ###

ISSUE 104. utility to generate DynamicProtobuf for given PigSchema (Bill Graham)

ISSUE 100. Front page doc update (Dmitriy Ryaboy)

### Version 2.1.0 ###

ISSUE 95. Allow enum for key field in Thrift maps (Raghu Angadi)

ISSUE 94. Update WritableLoadCaster to work with pig0.9 (Jonathan Coveney)

ISSUE 91. Add getMessageFieldNames() method to Protobuf utils (Bill Graham)

### Version 2.0.9 ###

ISSUE 89. Improve interface and Null handling in SequenceFileStorage (Andy Schlaikjer)

ISSUE 88. Better error messages while storing Thrift or Protobufs (Andy Schlaikjer)

### Version 2.0.8 ###

ISSUE 86. log serialization errors in lzo loaders (rangadi)

ISSUE 85. fix for SequenceFileLoader getWritableConverter (Andy Schlaikjer)

ISSUE 84. various SequenceFileLoader updates (Andy Schlaikjer)

ISSUE 82. maven-install ant target (Andy Schlaikjer)

ISSUE 81. Fix error rate calculation in LzoRecordReader (rangadi)

ISSUE 80. varargs constroctor for WritableConverters in SequenceFileLoader (Andy Schlaikjer)

### Version 2.0.7 ###

ISSUE 79. Fix schema for DataBag in Pig 0.9 (rangadi)

ISSUE 78. Thrift and Protobuf converters for SequenceFile storage (Andy Schlaikjer)

ISSUE 77. Support passing arguments to converters in SequenceFile storage (Andy Schlaikjer)

ISSUE 75. Fix check for empty line base64 line loaders (rangadi)

### Version 2.0.6 ###

ISSUE 74. Allow a few bad records in LzoRecordReders (rangadi)

ISSUE 73. SequenceFile Pig storage (Andy Schlaikjer)

ISSUE 72. Write lzo index file to a temp file (rangadi)

### Version 2.0.5 ###

ISSUE 68. Fix isSplittable() in LzoInputFormat (rangadi)

ISSUE 64. Support for lzo index files in LzoOutputFormat (rangadi)

### Version 2.0.4 ###

ISSUE 67. Reduce memory footprint of LzoInputFormat (dvryaboy)

### Version 2.0.3 ###

ISSUE 59. check if PigReporter returns a null counter (dvryaboy)

ISSUE 57. fix recursion while listing lzo files (rangadi)

ISSUE 55. code cleanup for Pig storage classes (rangadi)

### Version 2.0.2 ###

ISSUE 54. Fix Thrift and Protobuf Input/Output format initialization in Pig (rangadi)

ISSUE 52. Fix Thrift and Protobuf PigStorage (dvryaboy)

### Version 2.0.1 ###

ISSUE 51. Fix a regression in ISSUE-49 for Thrift loaders/inputformats.

ISSUE 51. Add HBaseLoader for backward compatibility (rangadi).

### Version 2.0.0 ###

NO TICKET. Upgrade Pig compatibility to 0.8

### Version 1.2.6 ###

ISSUE 50. Add dynamic Thrift to Proto conversion (dvryaboy)

ISSUE 49. Add "deprecated" lzo b64 input/output support for Thrift (sigmoids via rangadi)

### Version 1.2.5 ###

ISSUE 46. ThriftToPig : don't wrap STRUCT in another tuple for schema. (rangadi)

ISSUE 43. NPE in BlockWriter. (rangadi)

ISSUE 42. Couple of useful tweaks to DeprecatedLzoInputFormats. (avibryant)

ISSUE 40. Fix ThriftUtils use of classLoader. (rangadi)

NO TICKET. Use generic protoloader instead of requiring pre-generated ProtobufLoaders. (dvryaboy)

### Version 1.2.4 ###

ISSUE 38. Thrift-To-Pig reimplementation. (rangadi)

ISSUE 37. upgrade common-codec from 1.3 to 1.4 to explictly eliminate misuse. (angushe)

ISSUE 36. Use Pig class loader in Lzo Pig loader/storage (rangadi)

### Version 1.2.3 ###

ISSUE 35.  Fixes to B64Line Pig Loader.

ISSUE 34.  Typos in "Thrift"

### Version 1.2.2 ###

NO TICKET. Protobuf Pig storage : do not reuse builder object.

### Version 1.2.1 ###

ISSUE 28. Equals is now consistent with CompareTo for ProtobufWritables (rangadi)

ISSUE 30.  Handle Boolean and Bytes in ProtobufStorage, and add generators. (rangadi) 

ISSUE 30.  Handle Thrift binary and double types in ThriftToPig schema (rangadi)

ISSUE 30.  Deprecated Json Input format and Thrift byte to Tuple UDF (rangadi)

NO TICKET. Add JsonLoader (without LZO) and JsonStringToMap UDF (dvryaboy)

ISSUE 28. Make ProtobufWritable have stable hashCode() implementation (dvryaboy)
