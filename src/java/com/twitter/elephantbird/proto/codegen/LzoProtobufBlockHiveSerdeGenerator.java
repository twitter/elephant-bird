package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.hive.serde.LzoProtobufBlockHiveSerde;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;

public class LzoProtobufBlockHiveSerdeGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/hive/serde/Lzo%sProtobufBlockHiveSerde.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.hive.serde;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s.mapreduce.io.Protobuf%sWritable;", packageName_, descriptorProto_.getName()).endl();
    sb.append("import org.apache.hadoop.hive.serde2.SerDeException;").endl();
    sb.append("import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;").endl();
    sb.append("import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;").endl();
    sb.append("import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;").endl();
    sb.append("import org.apache.hadoop.io.Writable;").endl();
    sb.append("import %s;", LzoProtobufBlockHiveSerde.class.getName()).endl().endl();

    sb.append("public class Lzo%sProtobufBlockHiveSerde extends LzoProtobufBlockHiveSerde {", descriptorProto_.getName()).endl();
    
    sb.append("  public ObjectInspector getObjectInspector() throws SerDeException {").endl();
    sb.append("    return ObjectInspectorFactory.getReflectionObjectInspector(%s.class, ObjectInspectorOptions.PROTOCOL_BUFFERS);", descriptorProto_.getName()).endl();
    sb.append("  }").endl().endl();
    
    sb.append("  public Object deserialize(Writable w) throws SerDeException {").endl();
    sb.append("    return ((Protobuf%sWritable) w).get();", descriptorProto_.getName()).endl();
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
