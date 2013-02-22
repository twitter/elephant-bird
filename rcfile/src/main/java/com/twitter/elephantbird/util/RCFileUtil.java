package com.twitter.elephantbird.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.data.proto.Misc.ColumnarMetadata;

public class RCFileUtil {

  private static final Logger LOG = LoggerFactory.getLogger(RCFileUtil.class);

  /**
   * comma separated list of indices of the fields. This is not a list of field
   * numbers (as in a Protobuf or a Thrift class).
   *
   * If this configuration is not set or is empty, all the fields
   * are read ("unknown fields" in Protobufs are not carried over).
   */
  public static String REQUIRED_FIELD_INDICES_CONF =
                           "elephantbird.rcfile.input.required.field.indices";

  public static String COLUMN_METADATA_PROTOBUF_KEY = "elephantbird.rcfile.column.info.protobuf";

  /**
   * reads {@link ColumnarMetadata} stored in an RCFile.
   * @throws IOException if metadata is not stored or in case of any other error.
   */
  public static ColumnarMetadata readMetadata(Configuration conf, Path rcfile)
                                              throws IOException {

    Metadata metadata = null;

    Configuration confCopy = new Configuration(conf);
    // set up conf to read all the columns
    ColumnProjectionUtils.setFullyReadColumns(confCopy);

    RCFile.Reader reader = new RCFile.Reader(rcfile.getFileSystem(confCopy),
                                             rcfile,
                                             confCopy);

    //ugly hack to get metadata. RCFile has to provide access to metata
    try {
      Field f = RCFile.Reader.class.getDeclaredField("metadata");
      f.setAccessible(true);
      metadata = (Metadata)f.get(reader);
    } catch (Throwable t) {
      throw new IOException("Could not access metadata field in RCFile reader", t);
    }

    reader.close();

    Text metadataKey = new Text(COLUMN_METADATA_PROTOBUF_KEY);

    if (metadata == null || metadata.get(metadataKey) == null) {
      throw new IOException("could not find ColumnarMetadata in " + rcfile);
    }

    return Protobufs.mergeFromText(ColumnarMetadata.newBuilder(),
                                   metadata.get(metadataKey)).build();
  }

  /**
   * Returns list of columns that need to be read from the RCFile.
   * These columns are the intersection of currently required columns and
   * columns stored in the file.
   * If any required column does not exist in the file, we need to read
   * the "unknown fields" column, which is usually the last last one.
   */
  public static ArrayList<Integer> findColumnsToRead(
                                         Configuration      conf,
                                         List<Integer>      currFieldIds,
                                         ColumnarMetadata   storedInfo)
                                         throws IOException {

    ArrayList<Integer> columnsToRead = Lists.newArrayList();

    // first find the required fields
    ArrayList<Integer> requiredFieldIds = Lists.newArrayList();
    String reqFieldStr = conf.get(RCFileUtil.REQUIRED_FIELD_INDICES_CONF, "");

    int numKnownFields = currFieldIds.size();

    if (reqFieldStr == null || reqFieldStr.equals("")) {
      for(int i=0; i<numKnownFields; i++) {
        requiredFieldIds.add(currFieldIds.get(i));
      }
    } else {
      for (String str : reqFieldStr.split(",")) {
        int idx = Integer.valueOf(str);
        if (idx < 0 || idx >= numKnownFields) {
          throw new IOException("idx " + idx + " is out of range for known fields");
        }
        requiredFieldIds.add(currFieldIds.get(idx));
      }
    }

    List<Integer> storedFieldIds = storedInfo.getFieldIdList();

    for(int i=0; i < storedFieldIds.size(); i++) {
      int sid = storedFieldIds.get(i);
      if (sid > 0 && requiredFieldIds.contains(sid)) {
        columnsToRead.add(i);
      }
    }

    // unknown fields : the required fields that are not listed in storedFieldIds
    String unknownFields = "";
    for(int rid : requiredFieldIds) {
      if (!storedFieldIds.contains(rid)) {
        unknownFields += " " + rid;
      }
    }

    if (unknownFields.length() > 0) {
      int last = storedFieldIds.size() - 1;
      LOG.info("unknown fields among required fileds :" + unknownFields);
      if (storedFieldIds.get(last) != -1) { // not expected
        throw new IOException("No unknowns column in in input");
      }
      columnsToRead.add(last);
    }

    LOG.info(String.format(
        "reading %d%s out of %d stored columns for %d required columns",
        columnsToRead.size(),
        (unknownFields.length() > 0 ? " (including unknowns column)" : ""),
        storedInfo.getFieldIdCount(),
        requiredFieldIds.size()));

    return columnsToRead;
  }

  /**
   * Sets {@link #REQUIRED_FIELD_INDICES_CONF} to list of indices
   * if requiredFieldList is not null.
   */
  public static void setRequiredFieldConf(Configuration conf,
                                          RequiredFieldList requiredFieldList) {

    // set required fields conf for RCFile[Protobuf|Thrift] input format

    if (requiredFieldList != null) {

      List<Integer> indices = Lists.newArrayList();
      for(RequiredField f : requiredFieldList.getFields()) {
        indices.add(f.getIndex());
      }

      conf.set(RCFileUtil.REQUIRED_FIELD_INDICES_CONF,
               Joiner.on(",").join(indices));
    }
  }
}
