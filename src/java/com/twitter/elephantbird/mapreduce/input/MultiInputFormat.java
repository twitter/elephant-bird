package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TBase;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephantbird.util.HadoopUtils;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

/**
 * The input could consist of heterogeneous mix of formats storing
 * compatible objects. Currently supported formats:
 *   <ol>
 *     <li> Lzo Block storage of Thrift and Protobuf objects
 *     <li> Lzo B64Line storage of Thrift and Protobuf objects
 *   </ol>
 *
 * <p>
 * A small fraction of bad records are tolerated. See {@link LzoRecordReader}
 * for more information on error handling.
 */
public class MultiInputFormat<M>
                extends LzoInputFormat<LongWritable, BinaryWritable<M>> {

  // TODO need handle multiple input formats in a job better.
  //      might be better to store classname in the input split rather than in config.
  private static String CLASS_CONF_KEY = "elephantbird.class.for.MultiInputFormat";

  private TypeRef<M> typeRef;

  public MultiInputFormat() {}

  public MultiInputFormat(TypeRef<M> typeRef) {
    this.typeRef = typeRef;
  }

  private static enum Format {
    LZO_BLOCK,
    LZO_B64LINE;
  };

  /**
   * Sets jobs input format to {@link MultiInputFormat} and stores
   * supplied clazz's name in job configuration. This configuration is
   * read on the remote tasks to initialize the input format correctly.
   */
  public static void setInputFormatClass(Class<?> clazz, Job job) {
    job.setInputFormatClass(MultiInputFormat.class);
    setClassConf(clazz, job.getConfiguration());
  }

  /**
   * Stores supplied class name in configuration. This configuration is
   * read on the remote tasks to initialize the input format correctly.
   */
  protected static void setClassConf(Class<?> clazz, Configuration conf) {
    HadoopUtils.setInputFormatClass(conf, CLASS_CONF_KEY, clazz);
  }

  @SuppressWarnings("unchecked") // return type is runtime dependent
  @Override
  public RecordReader<LongWritable, BinaryWritable<M>>
  createRecordReader(InputSplit split, TaskAttemptContext taskAttempt)
                     throws IOException, InterruptedException {
    Configuration conf = taskAttempt.getConfiguration();
    if (typeRef == null) {
      setTypeRef(conf);
    }
    Class<?> recordClass = typeRef.getRawClass();

    Format fileFormat = determineFileFormat(split, conf);

    // Protobuf
    if (Message.class.isAssignableFrom(recordClass)) {
      switch (fileFormat) {
      case LZO_BLOCK:
        return new LzoProtobufBlockRecordReader(typeRef);
      case LZO_B64LINE:
        return new LzoProtobufB64LineRecordReader(typeRef);
      }
    }

    /* when types other than protobuf and thrift are supported,
     * should use Class.forName("org.apache.thrift.TBase") instead
     * of TBase.class so the we don't require thrift in classpath
     * unless the recordClass is a thrift class.
     */

    // Thrift
    if (TBase.class.isAssignableFrom(recordClass)) {
      switch (fileFormat) {
      case LZO_BLOCK:
        return new LzoThriftBlockRecordReader(typeRef);
      case LZO_B64LINE:
        return new LzoThriftB64LineRecordReader(typeRef);
      }
    }

    throw new IOException( "could not determine reader for "
        + ((FileSplit)split).getPath() + " with class " + recordClass.getName());
  }

  /** set typeRef from conf */
  private void setTypeRef(Configuration conf) {
    String className = conf.get(CLASS_CONF_KEY);

    if (className == null) {
      throw new RuntimeException(CLASS_CONF_KEY + " is not set");
    }

    Class<?> clazz = null;
    try {
      clazz = conf.getClassByName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to instantiate class '" + className + "'", e);
    }

    typeRef = new TypeRef<M>(clazz){};
  }

  /**
   * Checks to see if the input records are stored as SerializedBlock.
   * The block format starts with {@link Protobufs#KNOWN_GOOD_POSITION_MARKER}.
   * Otherwise the input is assumed to be Base64 encoded lines.
   */
  private static Format determineFileFormat(InputSplit split,
                                            Configuration conf)
                                            throws IOException {
    FileSplit fileSplit = (FileSplit)split;

    Path file = fileSplit.getPath();

    /* we could have a an optional configuration that maps a regex on a
     * file name to a format. E.g. ".*-block.lzo" to LZO_BLOCK file.
     */

    // most of the cost is opening the file and
    // reading first lzo block (about 256k of uncompressed data)

    CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
    if (codec == null) {
      throw new IOException("No codec for file " + file + " found");
    }

    InputStream in = file.getFileSystem(conf).open(file);
    InputStream lzoIn = null;

    // check if the file starts with magic bytes for Block storage format.
    try {
      lzoIn = codec.createInputStream(in);

      for(byte magic : Protobufs.KNOWN_GOOD_POSITION_MARKER) {
        int b = lzoIn.read();
        if (b < 0 || (byte)b != magic) {
          return Format.LZO_B64LINE;
        }
      }
    } finally {
      IOUtils.closeStream(lzoIn);
      IOUtils.closeStream(in);
    }

    // the check passed
    return Format.LZO_BLOCK;
  }
}


