package com.twitter.elephantbird.proto.codegen;

import com.google.protobuf.DescriptorProtos.DescriptorProto;

/**
 * The base class to use with codegen'd protocol buffer based objects.  To use,
 * derive from this class, override getFilename with the filename you'd like to write,
 * and return the code you want to generate in your overridden generateCode method.
 * Then add the classname of your new derived class to a yml file that the
 * HadoopProtoCodeGenerator is invoked with, and your class will be called to
 * generate your code!
 *
 * See examples like LzoProtobufB64LineInputFormatGenerator for more.
 */
public abstract class ProtoCodeGenerator {
  protected String protoFilename_;
  protected String packageName_;
  protected DescriptorProto descriptorProto_;
  protected ProtoCodeGenOptions codeGenOptions_;

  /**
   * Configure the class with the relevant protocol buffer info
   * @param protoFilename the PascalCased filename of the input proto file, so for example
   * "path/to/my_file.proto" comes in here as "MyFile". If the user specified
   * option java_outer_classname = "MyClass";
   * in their .proto file, the value will be "MyClass" instead.
   * @param packageName the package name from the protocol buffer file.  If the user
   * specified
   * option java_package = "com.something.mypackage";
   * in their .proto file, the value will be "com.something.mypackage" instead.
   * @param proto the descriptor for the proto itself.
   */
  public void configure(String protoFilename, String packageName,
      DescriptorProto proto, ProtoCodeGenOptions codeGenOptions) {
    protoFilename_ = protoFilename;
    packageName_ = packageName;
    descriptorProto_ = proto;
    codeGenOptions_ = codeGenOptions;
  }

  /**
   * @return The filename for the code being generated
   */
  public abstract String getFilename();

  /**
   * @return The string of generated code.
   */
  public abstract String generateCode();
}
