package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapreduce.input.BinaryConverterProvider;
import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.GenericWritable;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.HadoopUtils;

import org.apache.hadoop.conf.Configuration;

/**
 * Generic OutputFormat for records to be stored as lzo-compressed protobuf blocks.
 */
public class LzoGenericBlockOutputFormat<M> extends LzoOutputFormat<M, GenericWritable<M>> {

  private static String CLASS_CONF_KEY = "elephantbird.class.for.LzoGenericBlockOutputFormat";
  private static String GENERIC_ENCODER_KEY = "elephantbird.encoder.class.for.LzoGenericBlockOutputFormat";

  public static void setGenericConverterClassConf(Class<?> clazz, Configuration conf) {
    HadoopUtils.setClassConf(conf, GENERIC_ENCODER_KEY, clazz);
  }

  public static void setClassConf(Class<?> clazz, Configuration conf) {
    HadoopUtils.setClassConf(conf, CLASS_CONF_KEY, clazz);
  }

  @Override
  public RecordWriter<M, GenericWritable<M>> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {
    Configuration conf = HadoopCompat.getConfiguration(job);
    String encoderClassName = conf.get(GENERIC_ENCODER_KEY);
    Class<?> typeRef = null;
    Class<?> encoderClazz = null;
    BinaryConverterProvider<?> converterProvider = null;
    try {
      String typeRefClass = conf.get(CLASS_CONF_KEY);
      typeRef = conf.getClassByName(typeRefClass);
      encoderClazz = conf.getClassByName(encoderClassName);
      converterProvider = (BinaryConverterProvider<?>)encoderClazz.newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException("failed to instantiate class '" + encoderClassName + "'", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    BinaryConverter<?> converter = converterProvider.getConverter(conf);

    return new LzoBinaryBlockRecordWriter<M, GenericWritable<M>>(new BinaryBlockWriter(
        getOutputStream(job), typeRef, converter));
  }
}
