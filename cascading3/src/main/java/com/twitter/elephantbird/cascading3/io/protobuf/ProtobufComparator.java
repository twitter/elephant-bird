package com.twitter.elephantbird.cascading2.io.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;

import com.google.protobuf.Message;

import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.BufferedInputStream;

/**
 * Comparator for protobufs
 * @author Ning Liang
 */
public class ProtobufComparator implements StreamComparator<BufferedInputStream>,
    Comparator<Message> {

  private static final int BUFFER_SIZE = 1024;

  private byte[] buffer = new byte[BUFFER_SIZE];
  private NoCopyByteArrayOutputStream leftBos = new NoCopyByteArrayOutputStream();
  private NoCopyByteArrayOutputStream rightBos = new NoCopyByteArrayOutputStream();

  @Override
  public int compare(BufferedInputStream leftStream, BufferedInputStream rightStream) {
    leftBos.reset();
    rightBos.reset();

    readFully(leftStream, leftBos, buffer);
    readFully(rightStream, rightBos, buffer);

    return compareByteArrays(leftBos, rightBos);
  }

  @Override
  public int compare(Message left, Message right) {
    try {
      leftBos.reset();
      rightBos.reset();
      left.writeTo(leftBos);
      right.writeTo(rightBos);
      return compareByteArrays(leftBos, rightBos);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Method used to read a protobuf input stream into a byte array
   * @param in the protobuf input stream
   * @param out the byte output stream
   * @param buffer the array out output bytes
   */
  public static void readFully(InputStream in, ByteArrayOutputStream out, byte[] buffer) {
    try {
      int numRead = 0;
      while ((numRead = in.read(buffer, 0, buffer.length)) != -1) {
        out.write(buffer, 0, numRead);
      }
      out.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static int compareByteArrays(NoCopyByteArrayOutputStream leftBos,
      NoCopyByteArrayOutputStream rightBos) {
    byte[] left = leftBos.getByteArray();
    byte[] right = rightBos.getByteArray();

    int leftCount = leftBos.getCount();
    int rightCount = rightBos.getCount();

    if (leftCount != rightCount) {
      return leftCount - rightCount;
    } else {
      for (int i = 0; i < leftCount; i++) {
        if (left[i] != right[i]) {
          return left[i] - right[i];
        }
      }
      return 0;
    }
  }

  /**
   * Private class that holds a no copy version of a ByteArrayOutputStream
   */
  private static class NoCopyByteArrayOutputStream extends ByteArrayOutputStream {
    public byte[] getByteArray() {
      return buf;
    }

    public int getCount() {
      return count;
    }
  }

}
