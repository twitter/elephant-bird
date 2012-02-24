package com.twitter.elephantbird.proto;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.compiler.Plugin.CodeGeneratorRequest;
import com.google.protobuf.compiler.Plugin.CodeGeneratorResponse;
import com.google.protobuf.compiler.Plugin.CodeGeneratorResponse.File;
import com.twitter.elephantbird.proto.codegen.ProtoCodeGenOptions;
import com.twitter.elephantbird.proto.codegen.ProtoCodeGenerator;
import com.twitter.elephantbird.proto.codegen.ProtobufExtensionRegistryGenerator;
import com.twitter.elephantbird.proto.util.ProtogenHelper;
import com.twitter.elephantbird.util.Strings;

import net.sourceforge.yamlbeans.YamlException;
import net.sourceforge.yamlbeans.YamlReader;

public class HadoopProtoCodeGenerator {

  private static final String BUILTIN_KEY_OPTIONS = "__options__";

  public static void main(String[] args) throws IOException, YamlException {
    if (args.length < 1) {
      System.err.println("Usage: " + HadoopProtoCodeGenerator.class + " config_file");
      System.err.println("This expects a protocol buffer of type CodeGeneratorRequest to be given on stdin by protoc, " +
                         " and will print a protocol buffer of type CodeGeneratorResponse to stdout.");
      System.exit(1);
    }

    YamlReader reader = new YamlReader(new InputStreamReader(new FileInputStream(args[0])));
    Map<String, Object> config = reader.read(Map.class);

    ProtoCodeGenOptions codeGenOptions = new ProtoCodeGenOptions();
    if(config.containsKey(BUILTIN_KEY_OPTIONS)) {
      codeGenOptions.setOptions((Map<String, String>)config.get(BUILTIN_KEY_OPTIONS));
    }

    BufferedInputStream inputStream = new BufferedInputStream(System.in);
    CodeGeneratorRequest request = CodeGeneratorRequest.parseFrom(inputStream);
    CodeGeneratorResponse.Builder responseBuilder = CodeGeneratorResponse.newBuilder();

    Set<String> extensionNames = Collections.emptySet();
    if(codeGenOptions.isSupportExtension()) {
      extensionNames = HadoopProtoCodeGenerator.getAllExtensionNames(request);
    }

    for (FileDescriptorProto fileDescriptorProto: request.getProtoFileList()) {
      String protoName = ProtogenHelper.getProtoName(fileDescriptorProto);
      String packageName = ProtogenHelper.getPackageName(fileDescriptorProto);
      String protoFileKey = ProtogenHelper.getFilenameFromPath(fileDescriptorProto.getName());

      if (!config.containsKey(protoFileKey)) {
        System.err.println("No matching config file entries for key " + protoFileKey + ".");
        continue;
      }

      if(codeGenOptions.isSupportExtension()) {
        ProtobufExtensionRegistryGenerator extRegGenerator =
          new ProtobufExtensionRegistryGenerator(extensionNames);
        extRegGenerator.configure(protoName, packageName, null, codeGenOptions);
        System.err.println("Creating " + extRegGenerator.getFilename());
        responseBuilder.addFile(File.newBuilder()
            .setName(extRegGenerator.getFilename())
            .setContent(extRegGenerator.generateCode())
            .build());
      }

      List<String> codeGenClasses = (List<String>)config.get(protoFileKey);

      for (DescriptorProto descriptorProto: fileDescriptorProto.getMessageTypeList()) {
        List<ProtoCodeGenerator> codeGenerators = Lists.transform(codeGenClasses,
            createCodeGenerator(protoName, packageName, descriptorProto, codeGenOptions));

        for (ProtoCodeGenerator codeGenerator: codeGenerators) {
          if(codeGenerator.getFilename() == null) {
            continue;
          }
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

  private static Function<String, ProtoCodeGenerator> createCodeGenerator(
      final String protoFilename, final String packageName,
      final DescriptorProto proto, final ProtoCodeGenOptions codeGenOptions) {
    return new Function<String, ProtoCodeGenerator>() {
      @Override
      public ProtoCodeGenerator apply(String classname) {
        try {
          Class<? extends ProtoCodeGenerator> codeGenClass = Class.forName(classname).asSubclass(ProtoCodeGenerator.class);
          ProtoCodeGenerator codeGenerator = codeGenClass.newInstance();
          codeGenerator.configure(protoFilename, packageName, proto, codeGenOptions);
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


  private static Set<String> getAllExtensionNames(CodeGeneratorRequest request) {
    Set<String> extensionNames = new HashSet<String>();
    for(FileDescriptorProto e: request.getProtoFileList()) {
      String enclosingClassName = ProtogenHelper.getPackageName(e) + "." + ProtogenHelper.getProtoName(e);
      for(DescriptorProto dp: e.getMessageTypeList()) {
        HadoopProtoCodeGenerator.populateExtensionNames(extensionNames, dp, enclosingClassName);
      }
      for(FieldDescriptorProto fdp: e.getExtensionList()) {
        HadoopProtoCodeGenerator.populateExtensionNames(extensionNames, fdp, enclosingClassName);
      }
    }

    return extensionNames;
  }

  private static void populateExtensionNames(Set<String> extensionNames,
      DescriptorProto descriptorProto,String enclosingClassName) {
    String newEnclosingClassName = enclosingClassName + "." + descriptorProto.getName();

    for(FieldDescriptorProto fdp: descriptorProto.getExtensionList()) {
      HadoopProtoCodeGenerator.populateExtensionNames(extensionNames, fdp,
          newEnclosingClassName);
    }

    for(DescriptorProto dp: descriptorProto.getNestedTypeList()) {
      HadoopProtoCodeGenerator.populateExtensionNames(extensionNames, dp,
          newEnclosingClassName);
    }
  }

  private static void populateExtensionNames(Set<String> extensionNames,
      FieldDescriptorProto fieldDescriptorProto, String enclosingClassName) {
    Preconditions.checkArgument(!fieldDescriptorProto.getExtendee().isEmpty());
    extensionNames.add(enclosingClassName + "." + Strings.camelize(fieldDescriptorProto.getName(), true));
  }
}
