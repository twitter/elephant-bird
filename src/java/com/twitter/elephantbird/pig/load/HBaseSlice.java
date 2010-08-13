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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.Slice;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * HBase Slice to load a portion of range of a table. The key range will be
 * [start, end) Modeled from org.apache.hadoop.hbase.mapred.TableSplit.
 */
public class HBaseSlice implements Slice {

  /** A Generated Serial Version UID **/
  private static final long serialVersionUID = 9035916017187148965L;
  private static final Log LOG = LogFactory.getLog(HBaseSlice.class);
  private transient PigCounterHelper counterHelper_;

  // assigned during construction
  /** Table Name **/
  private final byte[] tableName_;
  /** Table Start Row **/
  private final byte[] startRow_;
  /** Table End Row **/
  private final byte[] endRow_;
  /** Table Region Location **/
  private final String regionLocation_;
  /** Input Columns **/
  private final byte[][] inputColumns_;
  /** Whether the row should be loaded **/
  private final boolean loadRowKey_;

  /** BigInteger representations of row range */
  private final BigInteger bigStart_;
  private final BigInteger bigEnd_;
  private final BigDecimal bigRange_;


  private Map<CompareFilter.CompareOp, String> innerFilters_ = Maps.newHashMap();
  private long limit_ = -1;


  // created as part of init
  /** The connection to the table in Hbase **/
  private transient HTable m_table;
  /** The scanner over the table **/
  private transient ResultScanner m_scanner;
  private transient long seenRows_ = 0;

  private transient ArrayList<Object> mProtoTuple;

  /**
   * Record the last processed row, so that we can restart the scanner when an
   * exception happened during scanning a table
   */
  private transient byte[] m_lastRow_;

  /**
   * Constructor
   * 
   * @param tableName
   *            table name
   * @param startRow
   *            start now, inclusive
   * @param endRow
   *            end row, exclusive
   * @param inputColumns
   *            input columns
   * @param location
   *            region location
   */
  public HBaseSlice(byte[] tableName, byte[] startRow, byte[] endRow,
      byte[][] inputColumns, boolean loadRowKey, final String location) {
    tableName_ = tableName;
    startRow_ = startRow;
    endRow_ = endRow;
    inputColumns_ = inputColumns;
    regionLocation_ = location;
    loadRowKey_ = loadRowKey;

    // We have to deal with different byte lengths of keys producing very different
    // BigIntegers (bigendianness is great this way). The code is mostly cribbed
    // from HBase's Bytes class.
    byte [] startPadded;
    byte [] endPadded;
    if (startRow.length < endRow.length) {
      startPadded = Bytes.padTail(startRow, endRow.length - startRow.length);
      endPadded = endRow;
    } else if (endRow.length < startRow.length) {
      startPadded = startRow;
      endPadded = Bytes.padTail(endRow, startRow.length - endRow.length);
    } else {
      startPadded = startRow;
      endPadded = endRow;
    }
    byte [] prependHeader = {1, 0};
    bigStart_ = new BigInteger(Bytes.add(prependHeader, startPadded));
    bigEnd_ = new BigInteger(Bytes.add(prependHeader, endPadded));
    bigRange_ = new BigDecimal(bigEnd_.subtract(bigStart_));
  }

  public void addFilter(CompareFilter.CompareOp compareOp, String filterValue) {
    innerFilters_.put(compareOp, filterValue);
  }

  /** @return table name */
  public byte[] getTableName() {
    return this.tableName_;
  }

  /** @return starting row key */
  public byte[] getStartRow() {
    return this.startRow_;
  }

  /** @return end row key */
  public byte[] getEndRow() {
    return this.endRow_;
  }

  /** @return input columns */
  public byte[][] getInputColumns() {
    return this.inputColumns_;
  }

  /** @return the region's hostname */
  public String getRegionLocation() {
    return this.regionLocation_;
  }

  @Override
  public long getStart() {
    // Not clear how to obtain this in a table...
    return 0;
  }

  @Override
  public long getLength() {
    // Not clear how to obtain this in a table...
    // it seems to be used only for sorting splits
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[] { regionLocation_ };
  }

  @Override
  public long getPos() throws IOException {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
  }

  @Override
  public float getProgress() throws IOException {

    // No way to know max.. just return 0. Sorry, reporting on the last slice is janky.
    // So is reporting on the first slice, by the way -- it will start out too high, possibly at 100%.
    if (endRow_.length==0) return 0;
    byte[] lastPadded = m_lastRow_;
    if (m_lastRow_.length < endRow_.length) {
      lastPadded = Bytes.padTail(m_lastRow_, endRow_.length - m_lastRow_.length);
    }
    if (m_lastRow_.length < startRow_.length) {
      lastPadded = Bytes.padTail(m_lastRow_, startRow_.length - m_lastRow_.length);
    }
    byte [] prependHeader = {1, 0};
    BigInteger bigLastRow = new BigInteger(Bytes.add(prependHeader, lastPadded));
    BigDecimal processed = new BigDecimal(bigLastRow.subtract(bigStart_));
    try {
      BigDecimal progress = processed.setScale(3).divide(bigRange_, BigDecimal.ROUND_HALF_DOWN);
      return progress.floatValue();
    } catch (java.lang.ArithmeticException e) {
      return 0;
    }
  }

  @Override
  public void init(DataStorage store) throws IOException {
    HBaseConfiguration conf = new HBaseConfiguration();
    // connect to the given table
    m_table = new HTable(conf, tableName_);
    // init the scanner
    initScanner();
  }

  /**
   * Init the table scanner
   * 
   * @throws IOException
   */
  private void initScanner() throws IOException {
    restart(startRow_);
    m_lastRow_ = startRow_;
  }

  /**
   * Restart scanning from survivable exceptions by creating a new scanner.
   * 
   * @param startRow
   *            the start row
   * @throws IOException
   */
  private void restart(byte[] startRow) throws IOException {
    Scan scan;
    if ((endRow_ != null) && (endRow_.length > 0)) {
      scan = new Scan(startRow, endRow_);
    } else {
      scan = new Scan(startRow);
    }

    // Set filters, if any.
    FilterList scanFilter = null;
    if (!innerFilters_.isEmpty()) {
      scanFilter = new FilterList();
      for (Map.Entry<CompareFilter.CompareOp, String>entry  : innerFilters_.entrySet()) {
        scanFilter.addFilter(new RowFilter(entry.getKey(), new BinaryComparator(Bytes.toBytesBinary(entry.getValue()) )));
      }
      scan.setFilter(scanFilter);
    }

    scan.addColumns(inputColumns_);
    this.m_scanner = this.m_table.getScanner(scan);
  }

  @Override
  public boolean next(Tuple value) throws IOException {
    Result result;
    try {
      result = m_scanner.next();
    } catch (UnknownScannerException e) {
      LOG.info("recovered from " + StringUtils.stringifyException(e));
      restart(m_lastRow_);
      if (m_lastRow_ != startRow_) {
        m_scanner.next(); // skip presumed already mapped row
      }
      result = this.m_scanner.next();
    }
    boolean hasMore = result != null && result.size() > 0 && (limit_ < 0 || limit_ > seenRows_);
    if (hasMore) {
      if (counterHelper_ == null) counterHelper_ = new PigCounterHelper();
      counterHelper_.incrCounter(HBaseSlice.class.getName(), Bytes.toString(tableName_) + " rows read", 1);
      m_lastRow_ = result.getRow();
      convertResultToTuple(result, value);
      seenRows_ += 1;
    }
    return hasMore;
  }

  /**
   * Convert a row result to a tuple
   * 
   * @param result
   *            row result
   * @param tuple
   *            tuple
   */
  private void convertResultToTuple(Result result, Tuple tuple) {
    if (mProtoTuple == null)
      mProtoTuple = new ArrayList<Object>(inputColumns_.length + (loadRowKey_ ? 1 : 0));

    if (loadRowKey_) {
      mProtoTuple.add(new DataByteArray(result.getRow()));
    }

    for (byte[] column : inputColumns_) {
      byte[] value = result.getValue(column);
      if (value == null) {
        mProtoTuple.add(null);
      } else {
        mProtoTuple.add(new DataByteArray(value));
      }
    }

    Tuple newT = TupleFactory.getInstance().newTuple(mProtoTuple);
    mProtoTuple.clear();
    tuple.reference(newT);
  }

  @Override
  public void close() throws IOException {
    if (m_scanner != null) {
      m_scanner.close();
      m_scanner = null;
    }
  }

  @Override
  public String toString() {
    return regionLocation_ + ":" + Bytes.toString(startRow_) + ","
    + Bytes.toString(endRow_);
  }

  public void setLimit(String limit) {
    LOG.info("Setting Slice limit to "+Long.valueOf(limit));
    limit_ = Long.valueOf(limit);
  }

}
