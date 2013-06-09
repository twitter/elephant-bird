# Elephant Bird #

### Version 4.1 ###

ISSUE 322. Re-implement ThriftBinaryDeserializer (Raghu Angadi)

ISSUE 321. Use reflection to handle Thrift structs better (Raghu Angadi)

ISSUE 320. Changes for using with cascading and MR1 in CDH4 (Rotem Hermon)

ISSUE 317. Enforce error threshold at the end of reading a split (Raghu Angadi)

### Version 4.0 ###

ISSUE 314. hadoop-compat module (Raghu Angadi)

ISSUE 313. IndexOutOfBounds Exception in RCFilePigStorage (Raghu Angadi)

ISSUE 308. Major update to dependencies. Most are in 'provided' scope. (Raghu Angadi)

ISSUE 308. Support Hadoop 1.x and 2.x (Raghu Angadi)

ISSUE 305. Lzo unit tests in Travis CI (Raghu Angadi)

### Version 3.0.9 ###

ISSUE 300. Better implementation of LuceneIndexCollectAllRecordReader (Alex Levenson)

ISSUE 297. Lucene: Fix query file constructor bug (Alex Levenson)

ISSUE 217. Add TypedProtobufWritable (Ximo Guanter)

### Version 3.0.8 ###

ISSUE 295. Lower required maven version to 3.0.0 (Travis Crawford)

ISSUE 293. Require maven 3.x (David Wu)

ISSUE 292. Working Travis CI (David Wu)

ISSUE 289. Thrift: Make list of enum values available (Julien Le Dem)

ISSUE 288. Thrift: Make requirementType in FieldMetaData available (Julien Le Dem)

### Version 3.0.7 ###

ISSUE 286. catch protobuf errors during block decoding (Aaron Siegel)

ISSUE 285. Handle runtime exceptions in ThritConverter (Raghu Angadi)

ISSUE 283. Add LocationAsTuple pig loader (Dmitriy Ryaboy)

ISSUE 282. Bugfixes and minor improvements for elephant-bird-lucene (Alex Levenson)

ISSUE 281. Add BytesWritableConverter (Andy Schlaikjer)

ISSUE 280. Add lucene docs to readme (Alex Levenson)

### Version 3.0.6 ###

ISSUE 276. Lucene support in MR and Pig (Alex Levenson)

ISSUE 275. Make PigToThrift.toThriftValue public (Bill Graham)

### Version 3.0.5 ###

ISSUE 274. Correct 3.0.4 version number in changes log (Travis Crawford)

ISSUE 271. Update protobuf deserializer to present enums as strings (Feng Peng)

ISSUE 270. Add LzoByteArrayScheme (Sam Ritchie)

ISSUE 269. Adds Travis CI build link to readme (Andy Schlaikjer)

ISSUE 268. Adds travis-ci configuration (Andy Schlaikjer)

ISSUE 266. Improves RawSequenceFileRecordReader javadoc (Andy Schlaikjer)

### Version 3.0.4 ###

*** The issues starred below have introduced breaking API changes. Please make sure your java code and pig scripts are updated to use changed APIs correctly.

ISSUE 257. Adds LzoRawBytesStorage to mirror LzoRawBytesLoader (Andy Schlaikjer)

ISSUE 256. *** Refactors SequenceFile{Loader, Storage}. SequenceFileStorage no longer extends SequenceFileLoader. (Andy Schlaikjer)

### Version 3.0.3 ###

ISSUE 245. Stop using DefaultDataBags, use NonSpillable ones instead (Dmitriy Ryaboy)

ISSUE 244. Fixing mvn package build error because of log4j (Eli Finkelshteyn)

ISSUE 243. Fix how SequenceFileLoader initializes FileSystem (Raghu Angadi)

ISSUE 242. Handle null values during Protobuf to tuple conversion (Raghu Angadi)

ISSUE 241. Default values during RCFile protobuf columns to tuple conversion (Raghu Angadi)

ISSUE 239. Set read limit while deserializing Thrift objects (Raghu Angadi)

ISSUE 238. Updates logging deps, normalizing on slf4j (Andy Schlaikjer)

ISSUE 237. Avoid repeating some of dependencies in sub modules (Raghu Angadi)

ISSUE 226. Improve handling of pig to thrift enum value conversion failure (Andy Schlaikjer)

### Version 3.0.2 ###

ISSUE 235. Update build so LZO tests work correctly (Travis Crawford)

ISSUE 234. Hive protobuf deserializer (Raghu Angadi)

ISSUE 233. Redo classloader checks (Raghu Angadi)

ISSUE 230. Add DeprecatedRawMultiInputFormat (Travis Crawford)

ISSUE 231. Removes m2e filtering of build-helper-maven-plugin (Andy Schlaikjer)

ISSUE 232. merge two versions of TestThriftToPig (Raghu Angadi)

ISSUE 229. Lzo raw input format (Raghu Angadi)

ISSUE 227. More class visibility fixes (Raghu Angadi)

ISSUE 225. fix subclass check in MultiInputFormat (Raghu Angadi)

ISSUE 224. change log for 3.0.1 (Raghu Angadi)

### Version 3.0.1 ###

ISSUE 223. handle null keys in Thrift maps (Raghu Angadi)

ISSUE 222. use correct classloader for user supplied classes (Raghu Angadi)

### Version 3.0.0 ###

ISSUE 201. mavenize and split into multiple modules (Johan Oskarsson, Andy Schlaikjer)

### Version 2.2.3 ###

ISSUE 205. Set map value field alias to null (Andy Schlaikjer)

ISSUE 203. Remove yamlbeans from dependencies (Raghu Angadi)

ISSUE 202. remove Protobuf codegen utils, use maven repo for Thrift and Protobuf jars (Raghu Angadi)

ISSUE 198. Remove piggybank dependency. (Raghu Angadi)

ISSUE 200. Fix typos in VectorWritableConverter javadoc (Andy Schlaikjer)

ISSUE 197. Upgrade to cascading wip-288 (Argyris Zymnis)

### Version 2.2.2 ###

ISSUE 193. Fix schema for Thrift struct in a map. (Raghu Angadi)

### Version 2.2.1 ###

ISSUE 190. Added support for skipping EOFExceptions to SequenceFileLoader (Xavier Stevens)

ISSUE 191. Upgrade cascading to wip-281. (Argyris Zymnis)

ISSUE 189. make LzoBinaryBlockRecordReader return lzoblock offset (Yu Xu)

ISSUE 188. Pig schema for Thrift maps. (Raghu Angadi)

ISSUE 187. Adds check for input tuple length equal to 2, in case output schema validation is skipped (Andy Schlaikjer)

ISSUE 185. Add HCatalog support to HiveMultiInputFormat. (Travis Crawford)

ISSUE 183. Add a link to the mailing list on the readme. (Travis Crawford)

### Version 2.2.0 ###

ISSUE 176. Load nested JSON structures (André Panisson)

ISSUE 181. preserve unknown fields for Thrift + RCFile (Raghu Angadi)

ISSUE 180. Improves SequenceFileStorage schema validation (Andy Schlaikjer)

ISSUE 144. RCFile storage for Protobuf and Thrift (Raghu Angadi)

ISSUE 166. Hello, I've been asked to do a fatjar with all the compile deps of your project (Alexis Torres Paderewski)

ISSUE 179. Deprecated outputformat wrapper (Raghu Angadi)

ISSUE 178. remove DepcreateInputFormat and deprecated RecordReaders (Raghu Angadi)

ISSUE 177. Add Hive thrift support (Travis Crawford)

ISSUE 175. setClassConf() interface (Raghu Angadi)

ISSUE 174. Refactors VectorWritableConverter to support transformation of vector data on store (Andy Schlaikjer)

ISSUE 172. Adds outputSchema impl to JsonStringToMap UDF (Andy Schlaikjer)

ISSUE 173. Remove version number from readme file. The readme always describes the ... (Travis Crawford)

ISSUE 170. Cleanup cascading chemes: use deprecated input format wrapper (Argyris Zymnis)

ISSUE 168. Thrift : fix for isBuffer check (Raghu Angadi)

ISSUE 167. Deprecated inputformat wrapper (Raghu Angadi)

### Version 2.1.12 ###

### Version 2.1.11 ###

ISSUE 164. Mininum error threshold in LzoRecordReader (Raghu Angadi)

ISSUE 163. Support Thrift union (Raghu Angadi)

ISSUE 155. Remove non-null precondition in SequenceFileLoader (Andy Schlaikjer)

ISSUE 159. Inhibit class loading in RawSequenceFileRecordReader (Andy Schlaikjer)

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
