package com.twitter.elephantbird.pig.load;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.SeekableInputStream;
import org.apache.pig.backend.datastorage.SeekableInputStream.FLAGS;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.PigSlice;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.SampleLoader;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;

/*
 * This is a truly terrible thing.
 * All we want to do is pass the DataStorage parameter given to us in init() to the underlying loader:
 *     ((LzoSampleLoader)loader).setStorage(base);
 *
 * Unfortunately, due to PigSlice keeping all members private, this basically means we have to copy+paste
 * the whole thing.
 *
 * C'est la vie.
 */

public class StoragePassingPigSlice extends PigSlice {


  public StoragePassingPigSlice(String path, FuncSpec parser, long start, long length) {
    super(path, parser, start, length);
    this.file = path;
    this.start = start;
    this.length = length;
    this.parser = parser;
  }

  public FuncSpec getParser() {
    return parser;
  }

  public long getStart() {
    return start;
  }

  public long getLength() {
    return length;
  }

  public String[] getLocations() {
    return new String[] { file };
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(DataStorage base) throws IOException {
    if (getParser() == null) {
      loader = new PigStorage();
    } else {
      try {
        loader = (LoadFunc) PigContext.instantiateFuncFromSpec(getParser());
      } catch (Exception exp) {
        int errCode = 2081;
        String msg = "Unable to set up the load function.";
        throw new ExecException(msg, errCode, PigException.BUG, exp);
      }
    }
    fsis = base.asElement(base.getActiveContainer(), file).sopen();
    fsis.seek(getStart(), FLAGS.SEEK_CUR);

    end = getStart() + getLength();
    is = fsis;

    // set the right sample size
    try {
      PigContext pc = (PigContext) ObjectSerializer.deserialize(((HDataStorage)base).getConfiguration().getProperty("pig.pigContext"));
      ArrayList<Pair<FileSpec, Boolean>> inputs =
        (ArrayList<Pair<FileSpec, Boolean>>) ObjectSerializer.deserialize(((HDataStorage)base).getConfiguration().getProperty("pig.inputs"));

      ((SampleLoader)loader).computeSamples(inputs, pc);
    } catch (Exception e) {
      throw new ExecException(e.getMessage());
    }

    // This line is the reason we need a special class here.
    ((LzoSampleLoader)loader).setStorage(base);

    loader.bindTo(file.toString(), new BufferedPositionedInputStream(is,
        getStart()), getStart(), end);
  }

  public boolean next(Tuple value) throws IOException {
    Tuple t = loader.getNext();
    if (t == null) {
      return false;
    }
    value.reference(t);
    return true;
  }

  public long getPos() throws IOException {
    return fsis.tell();
  }

  public void close() throws IOException {
    is.close();
  }

  public float getProgress() throws IOException {
    float progress = getPos() - start;
    float finish = getLength();
    return progress / finish;
  }

  public void readFields(DataInput is) throws IOException {
    file = is.readUTF();
    start = is.readLong();
    length = is.readLong();
    parser = (FuncSpec)this.readObject(is);
  }

  public void write(DataOutput os) throws IOException {
    os.writeUTF(file);
    os.writeLong(start);
    os.writeLong(length);
    this.writeObject(parser, os);
  }

  private void writeObject(Serializable obj, DataOutput os)
  throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    byte[] bytes = baos.toByteArray();
    os.writeInt(bytes.length);
    os.write(bytes);
  }

  private Object readObject(DataInput is) throws IOException {
    byte[] bytes = new byte[is.readInt()];
    is.readFully(bytes);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
        bytes));
    try {
      return ois.readObject();
    } catch (ClassNotFoundException cnfe) {
      int errCode = 2094;
      String msg = "Unable to deserialize object.";
      throw new ExecException(msg, errCode, PigException.BUG, cnfe);
    }
  }


  // assigned during construction
  String file;
  long start;
  long length;
  FuncSpec parser;

  // Created as part of init
  private InputStream is;
  private SeekableInputStream fsis;
  private long end;
  private LoadFunc loader;

  private static final long serialVersionUID = 1L;
}
