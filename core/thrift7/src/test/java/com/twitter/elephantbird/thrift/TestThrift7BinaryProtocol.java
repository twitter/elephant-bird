package com.twitter.elephantbird.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class TestThrift7BinaryProtocol extends TestThriftBinaryProtocol {

  int METADATA_BYTES = 5; // type(1) + size(4)
  int MAP_METADATA_BYTES = 6; // key type(1) + value type(1) + size(4)

  @Test
  public void testCheckContainerSizeValidWhenCheckReadLength() throws TException {
    TTransport transport;
    ThriftBinaryProtocol protocol;

    transport = getMockTransport(3);
    replay(transport);
    protocol = new ThriftBinaryProtocol(transport);
    protocol.setReadLength(METADATA_BYTES + 3);
    protocol.readListBegin();
    verify(transport);

    transport = getMockTransport(3);
    replay(transport);
    protocol = new ThriftBinaryProtocol(transport);
    protocol.setReadLength(METADATA_BYTES + 3);
    protocol.readSetBegin();
    verify(transport);

    transport = getMockMapTransport(3);
    replay(transport);
    protocol = new ThriftBinaryProtocol(transport);
    protocol.setReadLength(MAP_METADATA_BYTES + 3);
    protocol.readMapBegin();
    verify(transport);
  }

  @Test(expected=TProtocolException.class)
  public void testCheckListContainerSizeInvalidWhenCheckReadLength() throws TException {
    TTransport transport = getMockTransport(400);
    replay(transport);
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(transport);
    protocol.setReadLength(METADATA_BYTES + 3);
    // this throws because size returned by Transport (400) > size per readLength (3)
    protocol.readListBegin();
    verify(transport);
  }

  @Test(expected=TProtocolException.class)
  public void testCheckSetContainerSizeInvalidWhenCheckReadLength() throws TException {
    TTransport transport = getMockTransport(400);
    replay(transport);
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(transport);
    // this throws because size returned by Transport (400) > size per readLength (3)
    protocol.setReadLength(METADATA_BYTES + 3);
    protocol.readSetBegin();
    verify(transport);
  }

  @Test(expected=TProtocolException.class)
  public void testCheckMapContainerSizeInvalidWhenCheckReadLength() throws TException {
    TTransport transport = getMockMapTransport(400);
    replay(transport);
    ThriftBinaryProtocol protocol = new ThriftBinaryProtocol(transport);
    // this throws because size returned by Transport (400) > size per readLength (3)
    protocol.setReadLength(MAP_METADATA_BYTES + 3);
    protocol.readMapBegin();
    verify(transport);
  }
}
