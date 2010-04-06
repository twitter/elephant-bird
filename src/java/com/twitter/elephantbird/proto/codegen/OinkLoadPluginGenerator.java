package com.twitter.elephantbird.proto.codegen;

import com.twitter.elephantbird.proto.util.FormattingStringBuffer;
import com.twitter.elephantbird.util.Strings;

public class OinkLoadPluginGenerator extends ProtoCodeGenerator {

  @Override
  public String getFilename() {
    return String.format("oink/%s/%s.rb",
        packageName_.replaceAll("\\.", "/"), Strings.tableize(descriptorProto_.getName()));
  }

  @Override
  public String generateCode() {
    FormattingStringBuffer sb = new FormattingStringBuffer();

    sb.append("require 'date_template_base'").endl();
    sb.endl();
    sb.append("module Oink").endl();
    sb.append("  module Plugins").endl();
    sb.append("    module Load").endl();
    sb.append("      # A class for locating data from the %s table", descriptorProto_.getName()).endl();
    sb.append("      class %s < DateTemplateBase", Strings.pluralize(descriptorProto_.getName())).endl();
    sb.append("        def initialize").endl();
    sb.append("        end").endl();
    sb.endl();
    sb.append("        def template_dir").endl();
    sb.append("          \"/tables/%s/%%Y/%%m/%%d\"", Strings.tableize(descriptorProto_.getName())).endl();
    sb.append("        end").endl();
    sb.append("      end").endl();
    sb.append("    end").endl();
    sb.append("  end").endl();
    sb.append("end").endl();
    sb.endl();

    return sb.toString();
  }
}
