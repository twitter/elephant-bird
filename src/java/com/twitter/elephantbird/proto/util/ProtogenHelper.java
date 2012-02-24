package com.twitter.elephantbird.proto.util;

import java.io.File;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.Strings;

public class ProtogenHelper {
  public static String getProtoNameFromFilename(String path) {
    return Strings.camelize(getFilenameFromPath(path));
  }

  public static String getFilenameFromPath(String path) {
    return removeExtension(new File(path).getName());
  }

  public static String removeExtension(String filename) {
    int dotIndex = filename.lastIndexOf('.');
    return dotIndex > 0 ? filename.substring(0, dotIndex) : filename;
  }

  public static Class<? extends Message> getProtoClass(String packageName,
      String protoFilename, String protoName) {
    String fullClassname = ProtogenHelper.getProtoClassFullName(packageName,
        protoFilename, protoName);
    return Protobufs.getInnerProtobufClass(fullClassname);
  }

  public static String getProtoClassFullName(String packageName,
      String protoFilename, String protoName) {
    return String.format("%s.%s.%s", packageName, protoFilename, protoName);
  }

  public static String getProtoTypeFullName(String packageName,
      String protoName) {
    return String.format("%s.%s", packageName, protoName);
  }

  public static String getProtoName(FileDescriptorProto fileDescriptorProto) {
      String protoName = ProtogenHelper.getProtoNameFromFilename(fileDescriptorProto.getName());
      if (fileDescriptorProto.getOptions().hasJavaOuterClassname()) {
        protoName = fileDescriptorProto.getOptions().getJavaOuterClassname();
      }
      return protoName;
  }

  public static String getPackageName(FileDescriptorProto fileDescriptorProto) {
      String packageName = fileDescriptorProto.getPackage();
      if (fileDescriptorProto.getOptions().hasJavaPackage()) {
        packageName = fileDescriptorProto.getOptions().getJavaPackage();
      }
      return packageName;
  }
}
