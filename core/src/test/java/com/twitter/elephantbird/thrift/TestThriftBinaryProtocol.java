package com.twitter.elephantbird.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

// mock transport for Set and List container types
class TestTransport extends TTransport {

  protected int containerSize;

  protected int readAllCalls = 0;

  // returns the supplied int as the container size
  // when called by ThriftBinaryProtocol
  public TestTransport(int containerSize) {
    this.containerSize = containerSize;
  }

  @Override
  public void close() {}

  @Override
  public boolean isOpen() { return true; }

  @Override
  public void open() throws TTransportException {}

  // should not be invoked in unit tests
  public int read(byte[] buf, int off, int len) throws TTransportException {
    throw new IllegalStateException();
  }

  // helper method to set container size correctly in the supplied byte array
  protected void setContainerSize(byte[] buf, int n) {
    byte[] b = ByteBuffer.allocate(4).putInt(n).array();
    for (int i = 0; i < 4; i++) {
      buf[i] = b[i];
    }
  }

  @Override
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    int bytesWritten = 0;
    switch(++readAllCalls) {
      case 1:
        // first call, set data type
        buf[0] = TType.BYTE;
        bytesWritten = 1;
        break;
      case 2:
        // second call, set container size
        setContainerSize(buf, containerSize);
        bytesWritten = 4;
        break;
      default:
        // only two calls are needed for List and Set metadata
        throw new IllegalStateException();
    }
    return bytesWritten;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {}
}

// mock transport for Map container type
class TestMapTransport extends TestTransport {

  public TestMapTransport(int containerSize) {
    super(containerSize);
  }

  @Override
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    int bytesWritten = 0;
    switch(++readAllCalls) {
      case 1:
        // first call, set key type
        buf[0] = TType.BYTE;
        bytesWritten = 1;
        break;
      case 2:
        // second call, set value type
        buf[0] = TType.BYTE;
        bytesWritten = 1;
        break;
      case 3:
        // third call, set container size
        setContainerSize(buf, containerSize);
        bytesWritten = 4;
        break;
      default:
        // only three calls are needed for Map metadata 
        throw new IllegalStateException();
    }
    return bytesWritten;
  }
}

public class TestThriftBinaryProtocol {

  int METADATA_BYTES = 5; // type(1) + size(4)
  int MAP_METADATA_BYTES = 6; // key type(1) + value type(1) + size(4)

  @Test
  public void testCheckContainerSizeValid() throws TException {
    // any non-negative value is considered valid when checkReadLength is not enabled
    ThriftBinaryProtocol protocol;
    protocol = new ThriftBinaryProtocol(new TestTransport(3));
    protocol.readListBegin();
    protocol = new ThriftBinaryProtocol(new TestTransport(3));
    protocol.readSetBegin();
    protocol = new ThriftBinaryProtocol(new TestMapTransport(3));
    protocol.readMapBegin();
  }

  @Test
  public void testCheckContainerSizeValidWhenCheckReadLength() throws TException {
    ThriftBinaryProtocol protocol;
    protocol = new ThriftBinaryProtocol(new TestTransport(3));
    protocol.setReadLength(METADATA_BYTES + 3);
    protocol.readListBegin();
    protocol = new ThriftBinaryProtocol(new TestTransport(3));
    protocol.setReadLength(METADATA_BYTES + 3);
    protocol.readSetBegin();
    protocol = new ThriftBinaryProtocol(new TestMapTransport(3));
    protocol.setReadLength(MAP_METADATA_BYTES + 3);
    protocol.readMapBegin();
  }

  @Test(expected=TProtocolException.class)
  public void testCheckListContainerSizeInvalid() throws TException {
    // any negative value is considered invalid when checkReadLength is not enabled
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(new TestTransport(-1));
    protocol.readListBegin();
  }

  @Test(expected=TProtocolException.class)
  public void testCheckSetContainerSizeInvalid() throws TException {
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(new TestTransport(-1));
    protocol.readSetBegin();
  }

  @Test(expected=TProtocolException.class)
  public void testCheckMapContainerSizeInvalid() throws TException {
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(new TestMapTransport(-1));
    protocol.readMapBegin();
  }

  @Test(expected=TProtocolException.class)
  public void testCheckListContainerSizeInvalidWhenCheckReadLength() throws TException {
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(new TestTransport(400));
    protocol.setReadLength(METADATA_BYTES + 3);
    // this throws because size returned by Transport (400) > size per readLength (3)
    protocol.readListBegin();
  }

  @Test(expected=TProtocolException.class)
  public void testCheckSetContainerSizeInvalidWhenCheckReadLength() throws TException {
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(new TestTransport(400));
    // this throws because size returned by Transport (400) > size per readLength (3)
    protocol.setReadLength(METADATA_BYTES + 3);
    protocol.readSetBegin();
  }

  @Test(expected=TProtocolException.class)
  public void testCheckMapContainerSizeInvalidWhenCheckReadLength() throws TException {
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(new TestMapTransport(400));
    // this throws because size returned by Transport (400) > size per readLength (3)
    protocol.setReadLength(MAP_METADATA_BYTES + 3);
    protocol.readMapBegin();
  }
}
