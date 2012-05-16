package com.twitter.elephantbird.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.thrift.Fixtures;
import org.apache.thrift.TException;
import org.junit.Test;

import thrift.test.OneOfEach;

import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.DynamicMessage;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.elephantbird.examples.proto.ThriftFixtures;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.ThriftToProto;

public class TestThriftToProto {

  @Test
  public void testThriftToProto() throws TException, IOException {
    OneOfEach ooe = Fixtures.oneOfEach;
    ThriftToProto<OneOfEach, ThriftFixtures.OneOfEach> thriftToProto =
      ThriftToProto.newInstance(ooe, ThriftFixtures.OneOfEach.newBuilder().build());
    ThriftFixtures.OneOfEach proto = thriftToProto.convert(ooe);
    assertEquals(ooe.im_true, proto.getImTrue());
    assertEquals(ooe.im_false, proto.getImFalse());
    assertEquals(ooe.a_bite, proto.getABite());
    assertEquals(ooe.integer16, proto.getInteger16());
    assertEquals(ooe.integer32, proto.getInteger32());
    assertEquals(ooe.integer64, proto.getInteger64());
    assertEquals(ooe.double_precision, proto.getDoublePrecision(), 0.00001);
    assertEquals(ooe.some_characters, proto.getSomeCharacters());
    assertEquals(ooe.zomg_unicode, proto.getZomgUnicode());
    assertEquals(ooe.what_who, proto.getWhatWho());

    assertEquals(new String(ooe.getBase64(), "UTF-8"), proto.getBase64().toStringUtf8());
  }

  @Test
  public void testThriftToDynamicProto() throws DescriptorValidationException {
    PhoneNumber thriftPhone = new PhoneNumber("123-34-5467");
    thriftPhone.type = PhoneType.HOME;
    ThriftToDynamicProto<PhoneNumber> thriftToProto = new ThriftToDynamicProto<PhoneNumber>(PhoneNumber.class);
    DynamicMessage msg = thriftToProto.convert(thriftPhone);
    assertEquals(thriftPhone.number, Protobufs.getFieldByName(msg, "number"));
    assertEquals(thriftPhone.type.toString(), Protobufs.getFieldByName(msg, "type"));
  }
}
