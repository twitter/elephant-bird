package com.twitter.elephantbird.proto;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.sourceforge.yamlbeans.YamlException;
import net.sourceforge.yamlbeans.YamlReader;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.compiler.Plugin.CodeGeneratorRequest;
import com.google.protobuf.compiler.Plugin.CodeGeneratorResponse;
import com.google.protobuf.compiler.Plugin.CodeGeneratorResponse.File;
import com.twitter.elephantbird.proto.codegen.ProtoCodeGenerator;
import com.twitter.elephantbird.proto.util.ProtogenHelper;
import com.twitter.elephantbird.util.Strings;

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

    Map<String, Set<String>> protoExtensionNames = buildProtobufExtensionNames(request);
    System.err.println(protoExtensionNames);

    for (FileDescriptorProto fileDescriptorProto: request.getProtoFileList()) {
      String protoName = ProtogenHelper.getProtoName(fileDescriptorProto);
      String packageName = ProtogenHelper.getPackageName(fileDescriptorProto);
      String protoFileKey = ProtogenHelper.getFilenameFromPath(fileDescriptorProto.getName());

      if (!config.containsKey(protoFileKey)) {
        System.err.println("No matching config file entries for key " + protoFileKey + ".");
        continue;
      }
      List<String> codeGenClasses = (List<String>)config.get(protoFileKey);

      for (DescriptorProto descriptorProto: fileDescriptorProto.getMessageTypeList()) {
        String fullClassName = ProtogenHelper.getProtoClassFullName(packageName, protoName, descriptorProto.getName());
        List<ProtoCodeGenerator> codeGenerators = Lists.transform(codeGenClasses,
            createCodeGenerator(protoName, packageName, descriptorProto,
                protoExtensionNames.get(fullClassName)));
//        System.err.println("-----------------------------------------");
//        System.err.println(descriptorProto);
//        if(descriptorProto.getExtensionCount() > 0) {
//          System.err.println("got extension");
//          System.err.println(descriptorProto.getExtensionList());
//        }
//        System.err.println("-----------------------------------------");

        for (ProtoCodeGenerator codeGenerator: codeGenerators) {
          System.err.println("Creating " + codeGenerator.getFilename());
          responseBuilder.addFile(File.newBuilder()
                                      .setName(codeGenerator.getFilename())
                                      .setContent(codeGenerator.generateCode())
                                      .build());
        }
      }
//      System.err.println("---------extension list-----------------");
//      for(FieldDescriptorProto e: fileDescriptorProto.getExtensionList()) {
////        e.getDescriptorForType().getContainingType().getClass()
//        System.err.println(e);
//      }
//      System.err.println("---------extension list-----------------");
    }

    CodeGeneratorResponse response = responseBuilder.build();
    System.err.println("Created " + response.getFileCount() + " files.");

    System.out.write(response.toByteArray());
  }

  private static Function<String, ProtoCodeGenerator> createCodeGenerator(
      final String protoFilename, final String packageName,
      final DescriptorProto proto, final Set<String> protoExtensionNames) {
    return new Function<String, ProtoCodeGenerator>() {
      @Override
      public ProtoCodeGenerator apply(String classname) {
        try {
          Class<? extends ProtoCodeGenerator> codeGenClass = Class.forName(classname).asSubclass(ProtoCodeGenerator.class);
          ProtoCodeGenerator codeGenerator = codeGenClass.newInstance();
          codeGenerator.configure(protoFilename, packageName, proto, protoExtensionNames);
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


  private static Map<String, Set<String>> buildProtobufExtensionNames(
      CodeGeneratorRequest request) {
    Map<String, Set<String>> protoExtensionNames = new HashMap<String, Set<String>>();
    for(FileDescriptorProto e: request.getProtoFileList()) {
      String enclosingName = ProtogenHelper.getPackageName(e) + "." + ProtogenHelper.getProtoName(e);
      for(DescriptorProto dp: e.getMessageTypeList()) {
        populateProtobufExtensions(dp, protoExtensionNames, enclosingName);
      }
      for(FieldDescriptorProto extension: e.getExtensionList()) {
        populateProtobufExtensions(extension, protoExtensionNames, enclosingName);
      }
    }

    return protoExtensionNames;
  }

  private static void populateProtobufExtensions(DescriptorProto descriptorProto,
      Map<String, Set<String>> protoExtensionNames, String enclosingClassName) {
    String newEnclosingClassName = enclosingClassName + "." + descriptorProto.getName();

    for(FieldDescriptorProto fdp: descriptorProto.getExtensionList()) {
      HadoopProtoCodeGenerator.populateProtobufExtensions(fdp, protoExtensionNames,
          newEnclosingClassName);
//      String extendee = HadoopProtoCodeGenerator.getExtendeeClassName(fdp);
//      Set<String> classNames = protoExtensionNames.get(extendee);
//      if(classNames == null) {
//        classNames = new TreeSet<String>();
//        protoExtensionNames.put(extendee, classNames);
//      }
//      classNames.add(newEnclosingClassName + "." + Strings.camelize(fdp.getName(), true));
    }

    for(DescriptorProto dp: descriptorProto.getNestedTypeList()) {
      HadoopProtoCodeGenerator.populateProtobufExtensions(dp, protoExtensionNames,
          newEnclosingClassName);
    }
  }

  private static void populateProtobufExtensions(FieldDescriptorProto fieldDescriptorProto,
      Map<String, Set<String>> protoExtensionNames, String enclosingClassName) {
    String extendee = HadoopProtoCodeGenerator.getExtendeeClassName(fieldDescriptorProto);
    assert(extendee != null);

    Set<String> extensionNames = protoExtensionNames.get(extendee);
    if(extensionNames == null) {
      extensionNames = new TreeSet<String>();
      protoExtensionNames.put(extendee, extensionNames);
    }
    extensionNames.add(enclosingClassName + "." + Strings.camelize(fieldDescriptorProto.getName(), true));
  }

  private static String getExtendeeClassName(FieldDescriptorProto fieldDescriptorProto) {
    return StringUtils.removeStart(fieldDescriptorProto.getExtendee(), ".");
  }
}
