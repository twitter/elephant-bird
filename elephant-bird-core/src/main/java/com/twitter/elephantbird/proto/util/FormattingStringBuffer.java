package com.twitter.elephantbird.proto.util;

/**
 * A hack on top of a StringBuffer (which unfortunately is a final class and can't
 * be extended) to let one pass format strings.  Also, I got sick of writing
 * sb.append("\n"), so I made sb.endl() in C++'s honor.
 */
public class FormattingStringBuffer {
  private StringBuffer sb_;

  public FormattingStringBuffer() {
    sb_ = new StringBuffer();
  }

  public FormattingStringBuffer append(String formatStr, Object ... args) {
    sb_.append(String.format(formatStr, args));
    return this;
  }

  public FormattingStringBuffer endl() {
    sb_.append("\n");
    return this;
  }

  @Override
  public String toString() {
    return sb_.toString();
  }
}
