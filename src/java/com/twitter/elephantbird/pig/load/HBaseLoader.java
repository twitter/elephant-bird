/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.mapreduce.input.LzoLineRecordReader;
import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

/**
 * A <code>Slicer</code> that splits the hbase table into {@link HBaseSlice}s.
 * Actual loading is done in {@link HBaseSlice}. Derived from the HbaseStorage implementation
 * in 0.6 Piggybank.
 * <br>
 * TODO: row version controls<br>
 */
public class HBaseLoader extends LoadFunc 
 {

  private static final Log LOG = LogFactory.getLog(HBaseLoader.class);


  private byte[][] cols_;
  private HTable table_;
  private final HBaseConfiguration conf_;
  private final boolean loadRowKey_;
  private final CommandLine configuredOptions_;
  private final static Options validOptions_ = new Options();
  private final static CommandLineParser parser_ = new GnuParser();

  private static void populateValidOptions() { 
    validOptions_.addOption("loadKey", false, "Load Key");
    validOptions_.addOption("gt", true, "Records must be greater than this value (binary, double-slash-escaped)");
    validOptions_.addOption("lt", true, "Records must be less than this value (binary, double-slash-escaped)");   
    validOptions_.addOption("gte", true, "Records must be greater than or equal to this value");
    validOptions_.addOption("lte", true, "Records must be less than or equal to this value");
    validOptions_.addOption("caching", true, "Number of rows scanners should cache");
    validOptions_.addOption("limit", true, "Per-region limit");
  }

  /**
   * Constructor. Construct a HBase Table loader to load the cells of the
   * provided columns.
   * 
   * @param columnList
   *            columnlist that is a presented string delimited by space.
   * @throws ParseException 
   */
  public HBaseLoader(String columnList) throws ParseException {
    this(columnList, "");
    LOG.info("no-arg constructor");
  }

  /**
   * 
   * @param columnList
   * @param optString Loader options. Known options:<ul>
   * <li>-loadKey=(true|false)  Load the row key as the first column
   * <li>-gt=minKeyVal
   * <li>-lt=maxKeyVal 
   * <li>-gte=minKeyVal
   * <li>-lte=maxKeyVal
   * <li>-caching=numRows  number of rows to cache (faster scans, more memory).
   * </ul>
   * @throws ParseException 
   */
  public HBaseLoader(String columnList, String optString) throws ParseException {
    populateValidOptions();
    String[] colNames = columnList.split(" ");
    String[] optsArr = optString.split(" ");
    try {
      configuredOptions_ = parser_.parse(validOptions_, optsArr);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "", validOptions_ );
      throw e;
    }
    loadRowKey_ = configuredOptions_.hasOption("loadKey");  
    cols_ = new byte[colNames.length][];
    for (int i = 0; i < cols_.length; i++) {
      cols_[i] = Bytes.toBytes(colNames[i]);
    }

    conf_ = new HBaseConfiguration();
  }

  

  private boolean skipRegion(CompareOp op, byte[] key, String option ) {
    if (option == null) return false;
    BinaryComparator comp = new BinaryComparator(Bytes.toBytesBinary(slashisize(option)));
    RowFilter rowFilter = new RowFilter(op, comp);
    return rowFilter.filterRowKey(key, 0, key.length);
  }

  /**
   * replace sequences of two slashes ("\\") with one slash ("\")
   * (not escaping a slash in grunt is disallowed, but a double slash doesn't get converted
   * into a regular slash, so we have to do it instead)
   * @param str
   * @return
   */
  private String slashisize(String str) {
    return str.replace("\\\\", "\\");
  }

  
  public void validate(DataStorage store, String tablename)
  throws IOException {
    ensureTable(tablename);
  }

  private void ensureTable(String tablename) throws IOException {
    LOG.info("tablename: " + tablename);

    // We're looking for the right scheme here (actually, we don't
    // care what the scheme is as long as it is one and it's
    // different from hdfs and file. If the user specified to use
    // the multiquery feature and did not specify a scheme we will
    // have transformed it to an absolute path. In that case we'll
    // take the last component and guess that's what was
    // meant. We'll print a warning in that case.
    int index;
    if(-1 != (index = tablename.indexOf("://"))) {
      if (tablename.startsWith("hdfs:") 
          || tablename.startsWith("file:")) {
        index = tablename.lastIndexOf("/");
        if (-1 == index) {
          index = tablename.lastIndexOf("\\");
        }

        if (-1 == index) {
          throw new IOException("Got tablename: "+tablename
              +". Either turn off multiquery (-no_multiquery)"
              +" or specify load path as \"hbase://<tablename>\".");
        } else {
          String in = tablename;
          tablename = tablename.substring(index+1);
          LOG.warn("Got tablename: "+in+" Assuming you meant table: "
              +tablename+". Either turn off multiquery (-no_multiquery) "
              +"or specify load path as \"hbase://<tablename>\" "
              +"to avoid this warning.");
        }
      } else {
        tablename = tablename.substring(index+3);
      }
    }

    if (table_ == null) {
      table_ = new HTable(conf_, tablename);
    }
  }

  // HBase LoadFunc
  // Most of the action happens in the Slice class.

  
  public void bindTo(String fileName, BufferedPositionedInputStream is,
      long offset, long end) throws IOException {
    // do nothing
  }

  
  public Schema determineSchema(String fileName, ExecType execType,
      DataStorage storage) throws IOException {
    // do nothing
    return null;
  }



  
  public Tuple getNext() throws IOException {
    // do nothing
    return null;
  }

  
  public String bytesToCharArray(byte[] b) throws IOException {
    return Bytes.toString(b);    
  }

  
  public Double bytesToDouble(byte[] b) throws IOException {
    if (Bytes.SIZEOF_DOUBLE > b.length){ 
      return Bytes.toDouble(Bytes.padHead(b, Bytes.SIZEOF_DOUBLE - b.length));
    } else {
      return Bytes.toDouble(Bytes.head(b, Bytes.SIZEOF_DOUBLE));
    }
  }

  
  public Float bytesToFloat(byte[] b) throws IOException {
    if (Bytes.SIZEOF_FLOAT > b.length){ 
      return Bytes.toFloat(Bytes.padHead(b, Bytes.SIZEOF_FLOAT - b.length));
    } else {
      return Bytes.toFloat(Bytes.head(b, Bytes.SIZEOF_FLOAT));
    }
  }

  
  public Integer bytesToInteger(byte[] b) throws IOException {
    if (Bytes.SIZEOF_INT > b.length){ 
      return Bytes.toInt(Bytes.padHead(b, Bytes.SIZEOF_INT - b.length));
    } else {
      return Bytes.toInt(Bytes.head(b, Bytes.SIZEOF_INT));
    }
  }

  
  public Long bytesToLong(byte[] b) throws IOException {
    if (Bytes.SIZEOF_LONG > b.length){ 
      return Bytes.toLong(Bytes.padHead(b, Bytes.SIZEOF_LONG - b.length));
    } else {
      return Bytes.toLong(Bytes.head(b, Bytes.SIZEOF_LONG));
    }
  }

  /**
   * NOT IMPLEMENTED
   */
   
   public Map<String, Object> bytesToMap(byte[] b) throws IOException {
     throw new ExecException("can't generate a Map from byte[]");
   }

   /**
    * NOT IMPLEMENTED
    */
   
   public Tuple bytesToTuple(byte[] b) throws IOException {
     throw new ExecException("can't generate a Tuple from byte[]");
   }

   /**
    * NOT IMPLEMENTED
    */
   
   public DataBag bytesToBag(byte[] b) throws IOException {
     throw new ExecException("can't generate DataBags from byte[]");
   }
   public void setLocation(String location, Job job)
   throws IOException {
 	  FileInputFormat.setInputPaths(job, location);
   }
   public InputFormat getInputFormat() {
       return null;
   }

   public void prepareToRead(RecordReader reader, PigSplit split) {
   }
}
