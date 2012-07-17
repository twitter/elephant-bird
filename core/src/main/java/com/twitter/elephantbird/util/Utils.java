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
   * Ensures the <code>classLoader</code> is 'consistent' with the original
   * class loader that created <code>existingClass</code>. Asserts<br>
   * <code>classLoader.loadClass(existingClass.getName()) == existingClass</code>.
   * <p>
   *
   * If classLoader fails to load the class, this returns silently.<br>
   * Throws a RuntimeException with detailed message if the consistency
   * check fails.
   *
   * @param existingClass
   * @param classLoader
   */
  public static void ensureClassLoaderConsistency(Class<?> existingClass,
                                                  ClassLoader classLoader) {
    Class<?> loadedClass;
    try {
      loadedClass = Class.forName(existingClass.getName(), true, classLoader);
    } catch (ClassNotFoundException e) {
      return; // let class loading fail some where else.
    }

    if (!loadedClass.equals(existingClass)) {
      throw new RuntimeException("The class loader is incosistent with the "
              + "class loader that initially loaded "
              + existingClass.getClass()
              + ". This can lead to various unexpected side effects.");

    }
  }
}
