package com.twitter.elephantbird.mapreduce.input;

import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Implementation of a Lucene {@link Directory} for reading indexes directly off HDFS.
 * Note: This implementation is READ ONLY, it cannot be used to write to HDFS.
 *       All non read only methods throw {@link UnsupportedOperationException}
 *
 * @author Jimmy Lin
 */
public class LuceneHdfsDirectory extends Directory {
  private static final String[] EMPTY_STRING_LIST = new String[0];
  private final FileSystem fs;
  private final Path dir;

  public LuceneHdfsDirectory(String name, FileSystem fs) {
    this.fs = Preconditions.checkNotNull(fs,
        "FileSystem provided to LuceneHdfsDirectory cannot be null");
    Preconditions.checkNotNull(name, "File name provided to LuceneHdfsDirectory cannot be null");
    dir = new Path(name);
    try {
      Preconditions.checkArgument(fs.exists(dir), "Directory: " + dir + " does not exist!");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public LuceneHdfsDirectory(Path path, FileSystem fs) {
    this.fs = Preconditions.checkNotNull(fs);
    dir = path;
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return fs.exists(new Path(dir, name));
  }

  @Override
  public long fileLength(String name) throws IOException {
    return fs.getFileStatus(new Path(dir, name)).getLen();
  }

  @Override
  public String[] listAll() throws IOException {
    FileStatus[] statuses = fs.listStatus(dir);

    // some versions of hadoop return null instead of an empty list
    // or throwing an exception for non-existent directories
    if (statuses == null) {
      if (fs.exists(dir)) {
        return EMPTY_STRING_LIST;
      } else {
        throw new IllegalArgumentException("Directory: " + dir + " does not exist!");
      }
    }

    String[] files = new String[statuses.length];

    for (int i = 0; i < statuses.length; i++) {
      files[i] = statuses[i].getPath().getName();
    }
    return files;
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return new HDFSIndexInput(new Path(dir, name).toString());
  }

  private class HDFSIndexInput extends IndexInput {
    private Path path;
    private final FSDataInputStream in;
    private String resourceDescription;

    protected HDFSIndexInput(String resourceDescription) throws IOException {
      super(resourceDescription);
      this.resourceDescription = resourceDescription;
      path = new Path(resourceDescription);
      this.in = fs.open(path);
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long getFilePointer() {
      try {
        return in.getPos();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long length() {
      try {
        return fs.getFileStatus(path).getLen();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public byte readByte() throws IOException {
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      // Important: use readFully instead of read.
      in.readFully(b, offset, len);
    }

    @Override
    public void seek(long pos) throws IOException {
      in.seek(pos);
    }

    @Override
    public IndexInput clone() {
      try {
        HDFSIndexInput copy = new HDFSIndexInput(resourceDescription);
        copy.seek(getFilePointer());
        return copy;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // This is a read only implementation, so the following methods are not supported

  @Override
  public void deleteFile(String name) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sync(Collection<String> strings) throws IOException {
    throw new UnsupportedOperationException();
  }
}
