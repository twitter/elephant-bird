package com.twitter.elephantbird.proto;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.compiler.Plugin.CodeGeneratorRequest;
import com.google.protobuf.compiler.Plugin.CodeGeneratorResponse;
import com.google.protobuf.compiler.Plugin.CodeGeneratorResponse.File;
import com.twitter.elephantbird.proto.codegen.ProtoCodeGenerator;
import com.twitter.elephantbird.proto.util.ProtogenHelper;
import net.sourceforge.yamlbeans.YamlException;
import net.sourceforge.yamlbeans.YamlReader;

public class HadoopProtoCodeGenerator {
  public static void main(String[] args) throws IOException, YamlException {
    if (args.length < 1) {
      System.err.println("Usage: " + HadoopProtoCodeGenerator.class + " config_file");
      System.err.println("This expects a protocol buffer of type CodeGeneratorRequest to be given on stdin by protoc, " +
                         " and will print a protocol buffer of type CodeGeneratorResponse to stdout.");
      System.exit(1);
    }

    YamlReader reader = new YamlReader(new InputStreamReader(new FileInputStream(args[0])));
    Map<String, Object> config = reader.read(Map.class);

    BufferedInputStream inputStream = new BufferedInputStream(System.in);
    CodeGeneratorRequest request = CodeGeneratorRequest.parseFrom(inputStream);
    CodeGeneratorResponse.Builder responseBuilder = CodeGeneratorResponse.newBuilder();

    for (FileDescriptorProto fileDescriptorProto: request.getProtoFileList()) {
      String protoName = ProtogenHelper.getProtoNameFromFilename(fileDescriptorProto.getName());
      if (fileDescriptorProto.getOptions().hasJavaOuterClassname()) {
        protoName = fileDescriptorProto.getOptions().getJavaOuterClassname();
      }
      String packageName = fileDescriptorProto.getPackage();
      if (fileDescriptorProto.getOptions().hasJavaPackage()) {
        System.err.flush();
        packageName = fileDescriptorProto.getOptions().getJavaPackage();
      }

      String protoFileKey = ProtogenHelper.getFilenameFromPath(fileDescriptorProto.getName());

      if (!config.containsKey(protoFileKey)) {
        System.err.println("No matching config file entries for key " + protoFileKey + ".");
        continue;
      }
      List<String> codeGenClasses = (List<String>)config.get(protoFileKey);

      for (DescriptorProto descriptorProto: fileDescriptorProto.getMessageTypeList()) {
        List<ProtoCodeGenerator> codeGenerators = Lists.transform(codeGenClasses,
            createCodeGenerator(protoName, packageName, descriptorProto));

        for (ProtoCodeGenerator codeGenerator: codeGenerators) {
          System.err.println("Creating " + codeGenerator.getFilename());
          responseBuilder.addFile(File.newBuilder()
                                      .setName(codeGenerator.getFilename())
                                      .setContent(codeGenerator.generateCode())
                                      .build());
        }
      }
    }

    CodeGeneratorResponse response = responseBuilder.build();
    System.err.println("Created " + response.getFileCount() + " files.");

    System.out.write(response.toByteArray());
  }

  private static Function<String, ProtoCodeGenerator> createCodeGenerator(final String protoFilename,
      final String packageName, final DescriptorProto proto) {
    return new Function<String, ProtoCodeGenerator>() {
      public ProtoCodeGenerator apply(String classname) {
        try {
          Class<? extends ProtoCodeGenerator> codeGenClass = Class.forName(classname).asSubclass(ProtoCodeGenerator.class);
          ProtoCodeGenerator codeGenerator = codeGenClass.newInstance();
          codeGenerator.configure(protoFilename, packageName, proto);
          return codeGenerator;
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException("Couldn't instantiate class " + classname, e);
        } catch (InstantiationException e) {
          throw new IllegalArgumentException("Couldn't instantiate class " + classname, e);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("Couldn't instantiate class " + classname, e);
        }
      }
    };
  }
}
