package com.twitter.elephantbird.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.twitter.elephantbird.mapreduce.input.BinaryConverterProvider;
import com.twitter.elephantbird.mapreduce.io.BinaryConverter;
import com.twitter.elephantbird.mapreduce.io.GenericBlockWriter;
import com.twitter.elephantbird.mapreduce.io.GenericWritable;
import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.HadoopUtils;

import org.apache.hadoop.conf.Configuration;

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
    Class<?> typeRef = null;
    String genericEncoder = conf.get(GENERIC_ENCODER_KEY);
    Class<?> encoderClazz = null;
    BinaryConverterProvider<?> converterProvider = null;
    try {
      String typeRefClass = conf.get(CLASS_CONF_KEY);
      typeRef = conf.getClassByName(typeRefClass);
      encoderClazz = conf.getClassByName(genericEncoder);
      converterProvider = (BinaryConverterProvider<?>)encoderClazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("failed to instantiate class '" + genericEncoder + "'", e);
    }

    BinaryConverter<?> converter = converterProvider.getConverter(conf);

    return new LzoBinaryBlockRecordWriter<M, GenericWritable<M>>(new GenericBlockWriter(
        getOutputStream(job), converter, typeRef));
  }
}
