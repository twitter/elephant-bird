package com.twitter.elephantbird.pig.piggybank;

import org.apache.commons.lang.time.StopWatch;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import com.twitter.elephantbird.pig.util.ProtobufTuple;

public class TimeProtoConversions {

  /**
   * @param args
   * @throws ExecException 
   */
  public static void main(String[] args) throws ExecException {
    int iterations = 100000;
    ProtobufToPig protoConv = new ProtobufToPig();
    for (int i = 0; i < iterations; i++) {
      Person proto = Fixtures.buildPersonProto();
      Tuple t = protoConv.toTuple(proto);
      t.get(0);
      t = new ProtobufTuple(proto);
      t.get(0);
    }
    StopWatch timer = new StopWatch();
    timer.start();
    for (int i = 0; i < iterations; i++) {
      Person proto = Fixtures.buildPersonProto();
      Tuple t = protoConv.toTuple(proto);
      t.get(0);
    }
    timer.split();
    System.err.println(timer.getSplitTime());
    timer.reset();
    timer.start();
    for (int i = 0; i < iterations; i++) {
      Person proto = Fixtures.buildPersonProto();
      Tuple t = new ProtobufTuple(proto);
      t.get(0);
    }
    timer.split();
    System.err.println(timer.getSplitTime());
    
  }

}
