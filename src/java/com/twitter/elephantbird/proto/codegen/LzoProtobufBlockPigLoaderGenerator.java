package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.pig.load.ProtobufPigLoader;
import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.TypeRef;

public class LzoProtobufBlockPigLoaderGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("%s/pig/load/Lzo%sProtobufBlockPigLoader.java",
        packageName_.replaceAll("\\.", "/"), descriptorProto_.getName());
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("package %s.pig.load;", packageName_).endl().endl();

    sb.append("import %s.%s.%s;", packageName_, protoFilename_, descriptorProto_.getName()).endl();
    sb.append("import %s.mapreduce.input.Lzo%sProtobufBlockInputFormat;", packageName_, descriptorProto_.getName()).endl();
    sb.append("import %s;", ProtobufPigLoader.class.getName()).endl();
    sb.append("import org.apache.hadoop.mapreduce.InputFormat;").endl();
    sb.append("import %s;", TypeRef.class.getName()).endl();
    if(codeGenOptions_.isSupportExtension()) {
      sb.append("import %s;", ProtobufExtensionRegistryGenerator.getGenClassName(
          packageName_, protoFilename_, true)).endl();
    }
    sb.endl();

    sb.append("public class Lzo%sProtobufBlockPigLoader extends ProtobufPigLoader<%s> {", descriptorProto_.getName(), descriptorProto_.getName()).endl();
    sb.append("  public Lzo%sProtobufBlockPigLoader() {", descriptorProto_.getName()).endl();
    if(codeGenOptions_.isSupportExtension()) {
      String extRegClassName = ProtobufExtensionRegistryGenerator.getGenClassName(
          packageName_, protoFilename_, false);
      sb.append("    super(new TypeRef<%s>(){}, %s.getInstance());",
          descriptorProto_.getName(), extRegClassName).endl();
    } else {
      sb.append("    super(new TypeRef<%s>(){});", descriptorProto_.getName()).endl();
    }
    sb.append("  }").endl();
    sb.append("}").endl();
    sb.endl();

    return sb.toString();
  }
}
