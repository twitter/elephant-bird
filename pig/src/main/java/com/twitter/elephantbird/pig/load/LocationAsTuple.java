package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.mapreduce.input.IntegerListInputFormat;

/**
 * Parses the "location" into a tuple by splitting on a delimiter, and returns it.
 * Handy for turning scalars into relations. For example:
 * <pre>{@code
 * languages = load 'en,fr,jp' using LocationAsTuple(',');
 * -- languages is ('en', 'fr', 'jp')
 * language_bag = foreach languages generate flatten(TOBAG(*));
 * -- language_bag is a relation with three rows, ('en'), ('fr'), ('jp')
 * }</pre>
 */
public class LocationAsTuple extends LoadFunc implements LoadMetadata {

  public static final String DATA_PROP = "data";
  final byte token;
  boolean returned = false;
  private String signature;

  public LocationAsTuple() {
    this("\t");
  }

  public LocationAsTuple(String s) {
     token = StorageUtil.parseFieldDel(s);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() throws IOException {
    return new IntegerListInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    if (!returned) {
      returned = true;
      String dataString = getUDFProperties().getProperty(DATA_PROP);
      LinkedList<String> tups = Lists.newLinkedList();
      int offset = 0;
      for (int i = 0; i < dataString.length(); i++) {
        if (dataString.charAt(i) == token) {
          tups.add(dataString.substring(offset, i));
          offset = i+1;
        }
      }
      tups.add(dataString.substring(offset, dataString.length()));
      return TupleFactory.getInstance().newTupleNoCopy(tups);
    } else {
      return null;
    }
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.signature = signature;
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir)
  throws IOException {
      return location;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    Properties p = getUDFProperties();
    p.setProperty(DATA_PROP, location);
  }

  /**
   * Returns UDFProperties based on <code>signature</code>.
   */
  private Properties getUDFProperties() {
      return UDFContext.getUDFContext()
          .getUDFProperties(this.getClass(), new String[] {signature});
  }

  @Override
  public void prepareToRead(RecordReader arg0, PigSplit arg1) throws IOException {
    // do nothing.
  }

  @Override
  public ResourceSchema getSchema(String filename, Job job) throws IOException {
    List<FieldSchema> fieldSchemas = Lists.newArrayList();
    for (int i = 0; i < filename.length(); i++) {
      if (filename.charAt(i) == token) {
        FieldSchema fieldSchema = new FieldSchema(null, DataType.CHARARRAY);
        fieldSchemas.add(fieldSchema);
      }
    }
    // add last element after the token
    FieldSchema fieldSchema = new FieldSchema(null, DataType.CHARARRAY);
    fieldSchemas.add(fieldSchema);
    return new ResourceSchema(new Schema(fieldSchemas));
  }

  @Override
  public String[] getPartitionKeys(String arg0, Job arg1) throws IOException {
    return null;
  }

  @Override
  public ResourceStatistics getStatistics(String arg0, Job arg1) throws IOException {
    return null;
  }

  @Override
  public void setPartitionFilter(Expression arg0) throws IOException {
    // not implemented
  }

}
