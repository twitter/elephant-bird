# Elephant Bird #

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
