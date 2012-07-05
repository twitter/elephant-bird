package com.twitter.elephantbird.util;

public class Utils {
  /**
   * returns Class.forName(className, true, classLoader). <br>
   * Throws a RuntimeExcepiton if the class is not found.
   *
   * @see {@link Class#forName(String, boolean, ClassLoader)
   */
  public static Class<?> classForName(String className, ClassLoader classLoader) {
    try {
      return Class.forName(className, true, classLoader);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("failed to load class " + className, e);
    }
  }

  /**
   * Loads a given class using classLoader. In some cases, the class returned
   * might be different if clazz was not loaded from the same classLoader.
   */
  public static <T> Class<T> loadClass(Class<T> clazz, ClassLoader classLoader) {
    @SuppressWarnings("unchecked")
    Class<T> retClass = (Class<T>) classForName(clazz.getName(), classLoader);
    return retClass;
  }
}
