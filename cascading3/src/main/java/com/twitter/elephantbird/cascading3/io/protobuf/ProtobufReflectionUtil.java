package com.twitter.elephantbird.cascading2.io.protobuf;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.protobuf.Message;

/**
 * Utility methods for reflection based protobuf deserialization
 * @author Ning Liang
 */
public final class ProtobufReflectionUtil {
  private ProtobufReflectionUtil() { }

  /**
   * Parse the method for a message
   * @param klass the class containing the message
   * @return the parsed method
   */
  public static Method parseMethodFor(Class<Message> klass) {
    try {
      return klass.getMethod("parseDelimitedFrom", new Class[] {InputStream.class });
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parse the message in a given InputStream using scpecified Method
   * @param parseMethod the method used for parsing
   * @param in the input stream
   * @return the parsed message
   */
  public static Message parseMessage(Method parseMethod, InputStream in) {
    try {
      return (Message) parseMethod.invoke(null, in);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parse the message in a given Message container
   * @param klass the class containing the message
   * @param in the input stream
   * @return the parsed Message
   */
  public static Message parseMessage(Class<Message> klass, InputStream in) {
    Method parseMethod = parseMethodFor(klass);
    return parseMessage(parseMethod, in);
  }
}
