package com.twitter.elephantbird.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.easymock.IAnswer;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class TestThriftBinaryProtocol {

  // helper method to set container size correctly in the supplied byte array
  protected void setContainerSize(byte[] buf, int n) {
    byte[] b = ByteBuffer.allocate(4).putInt(n).array();
    for (int i = 0; i < 4; i++) {
      buf[i] = b[i];
    }
  }

  protected void setDataType(byte[] buf) {
    buf[0] = TType.BYTE;
  }

  // mock transport for Set and List container types
  protected TTransport getMockTransport(final int containerSize) throws TException {
    TTransport transport = createStrictMock(TTransport.class);
    // not using buffered mode for tests, so return -1 per the contract
    expect(transport.getBytesRemainingInBuffer()).andReturn(-1);
    // first call, set data type
    expect(transport.readAll(isA(byte[].class), anyInt(), anyInt()))
      .andAnswer(
        new IAnswer<Integer>() {
          public Integer answer() {
            byte[] buf = (byte[])(EasyMock.getCurrentArguments()[0]);
            setDataType(buf);
            return 1;
          }
        }
      );
    expect(transport.getBytesRemainingInBuffer()).andReturn(-1);
    // second call, set container size
    expect(transport.readAll(isA(byte[].class), anyInt(), anyInt()))
      .andAnswer(
        new IAnswer<Integer>() {
          public Integer answer() {
            byte[] buf = (byte[])(EasyMock.getCurrentArguments()[0]);
            setContainerSize(buf, containerSize);
            return 4;
          }
        }
      );
    return transport;
  }

  // mock transport for Map container type
  protected TTransport getMockMapTransport(final int containerSize) throws TException {
    TTransport transport = createStrictMock(TTransport.class);
    // not using buffered mode for tests, so return -1 per the contract
    expect(transport.getBytesRemainingInBuffer()).andReturn(-1);
    // first call, set key type
    expect(transport.readAll(isA(byte[].class), anyInt(), anyInt()))
      .andAnswer(
        new IAnswer<Integer>() {
          public Integer answer() {
            byte[] buf = (byte[])(EasyMock.getCurrentArguments()[0]);
            setDataType(buf);
            return 1;
          }
        }
      );
    expect(transport.getBytesRemainingInBuffer()).andReturn(-1);
    // second call, set value type
    expect(transport.readAll(isA(byte[].class), anyInt(), anyInt()))
      .andAnswer(
        new IAnswer<Integer>() {
          public Integer answer() {
            byte[] buf = (byte[])(EasyMock.getCurrentArguments()[0]);
            setDataType(buf);
            return 1;
          }
        }
      );
    expect(transport.getBytesRemainingInBuffer()).andReturn(-1);
    // third call, set container size
    expect(transport.readAll(isA(byte[].class), anyInt(), anyInt()))
      .andAnswer(
        new IAnswer<Integer>() {
          public Integer answer() {
            byte[] buf = (byte[])(EasyMock.getCurrentArguments()[0]);
            setContainerSize(buf, containerSize);
            return 4;
          }
        }
      );
    return transport;
  }

  @Test
  public void testCheckContainerSizeValid() throws TException {
    // any non-negative value is considered valid when checkReadLength is not enabled
    TTransport transport;
    ThriftBinaryProtocol protocol;

    transport = getMockTransport(3);
    replay(transport);
    protocol = new ThriftBinaryProtocol(transport);
    protocol.readListBegin();
    verify(transport);

    transport = getMockTransport(3);
    replay(transport);
    protocol = new ThriftBinaryProtocol(transport);
    protocol.readSetBegin();
    verify(transport);

    transport = getMockMapTransport(3);
    replay(transport);
    protocol = new ThriftBinaryProtocol(transport);
    protocol.readMapBegin();
    verify(transport);
  }

  @Test(expected=TProtocolException.class)
  public void testCheckListContainerSizeInvalid() throws TException {
    // any negative value is considered invalid when checkReadLength is not enabled
    TTransport transport = getMockTransport(-1);
    replay(transport);
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(transport);
    protocol.readListBegin();
    verify(transport);
  }

  @Test(expected=TProtocolException.class)
  public void testCheckSetContainerSizeInvalid() throws TException {
    TTransport transport = getMockTransport(-1);
    replay(transport);
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(transport);
    protocol.readSetBegin();
    verify(transport);
  }

  @Test(expected=TProtocolException.class)
  public void testCheckMapContainerSizeInvalid() throws TException {
    TTransport transport = getMockMapTransport(-1);
    replay(transport);
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(transport);
    protocol.readMapBegin();
    verify(transport);
  }
}
