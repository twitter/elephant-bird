package com.twitter.elephantbird.mapreduce.input;

/**
* A {@link org.apache.hadoop.mapred.RecordReader} should implement 
* MapredInputFormatCompatible if it intends to be compatable 
* with {@link org.apache.hadoop.mapred.input.DeprecatedInputFormatWrapper}
* and {@link org.apache.hadoop.mapred.lib.CombinedFileInputFormat} 
*
* DeprecatedInputFormatWrapper enables you to use a mapreduce
* {@link org.apache.hadoop.mapreduce.InputFormat} in contexts
* where the old mapred interface is required. 
*
* RecordReaders written for the deprecated mapred interface reuse
* the key and value objects. This is not a requirement for
* the RecordReaders written for the newer mapreduce interface 
*
* This interface allows DeprecatedInputFormatWrapper to
* manually set key and value on the RecordReader to satisfy
* the old mapred interface.
*/
public interface MapredInputFormatCompatible<K, V> {
  /**
  * Set the RecordReader's existing key and value objects
  * to be equal to the key and value objects passed in.
  * 
  * When implemented, DeprecatedInputFormatWrapper calls 
  * this before every call to nextKeyValue().
  */
  public void setKeyValue(K key, V value);
}

