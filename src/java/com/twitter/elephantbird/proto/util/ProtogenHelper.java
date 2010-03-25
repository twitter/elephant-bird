package com.twitter.elephantbird.proto.util;

import java.io.File;

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
    String fullClassname = String.format("%s.%s.%s", packageName, protoFilename, protoName);
    return Protobufs.getInnerProtobufClass(fullClassname);
  }
}
