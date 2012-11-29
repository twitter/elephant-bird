package com.twitter.elephantbird.mapreduce.input;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Collection;

/**
 * Implementation of a Lucene {@link Directory} for reading indexes directly off HDFS.
 *
 * @author Jimmy Lin
 */
public class HdfsDirectory extends Directory {
  private final FileSystem fs;
  private final Path dir;

  public HdfsDirectory(String name, FileSystem fs) {
    this.fs = Preconditions.checkNotNull(fs);
    dir = new Path(name);
  }

  public HdfsDirectory(Path path, FileSystem fs) {
    this.fs = Preconditions.checkNotNull(fs);
    dir = path;
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }

  @Override
  public void deleteFile(String name) throws IOException {
    Path path = new Path(dir, name);
    fs.delete(path, false);
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

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  // TODO do we need to do anything here?
  @Override
  public void sync(Collection<String> strings) throws IOException { }

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

    // TODO find out if this is necessary anymore
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
}
