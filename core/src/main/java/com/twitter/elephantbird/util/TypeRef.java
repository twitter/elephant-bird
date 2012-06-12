package com.twitter.elephantbird.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Represents a generic type {@code T}.
 *
 * Neil Gafter's "Super Type Token" implementation. Taken from
 * <a href="http://gafter.blogspot.com/2006/12/super-type-tokens.html">here</a> and
 * <a href="http://gafter.blogspot.com/2007/05/limitation-of-super-type-tokens.html">here</a>.
 *
 * The class is abstract so that the user is required to create a subclass, typically an anonymous one via
 * TypeRef<MyClass> typeRef = new TypeRef<MyClass>(){};
 * Note the {} at the end, which makes this an anonymous subclass.  Then calling getGenericSuperclass
 * in the constructor returns the class in this file, from which we can inspect the type parameter.
 * See <a href="http://gafter.blogspot.com/2004/09/puzzling-through-erasure-answer.html">here</a> for
 * more about type erasure in Java.
 */
public abstract class TypeRef<T> {
  private final Type type_;
  private volatile Class class_;
  private volatile Constructor<?> constructor_;

  /**
   * Constructs a new generic type, deriving the generic type and class from
   * type parameter. Note that this constructor is protected, users should create
   * a (usually anonymous) subclass as shown above.
   *
   */
  protected TypeRef() {
    Type superclass = getClass().getGenericSuperclass();
    if (!(superclass instanceof ParameterizedType)) {
      throw new RuntimeException("Missing type parameter.");
    }
    ParameterizedType parameterized = (ParameterizedType) superclass;

    type_ = parameterized.getActualTypeArguments()[0];
  }

  /**
   * Constructs a new generic type, supplying the generic type
   * information and derving the class.
   *
   * @param genericType the generic type.
   * @throws IllegalArgumentException if genericType
   * is null or is neither an instance of Class or ParameterizedType whose raw
   * type is not an instance of Class.
   */
  public TypeRef(Type genericType) {
    if (genericType == null) {
      throw new IllegalArgumentException("Type must not be null");
    }

    type_ = genericType;
  }

  private static Class getClass(Type type) {
    if (type instanceof Class) {
      return (Class)type;
    } else if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType)type;
      if (parameterizedType.getRawType() instanceof Class) {
        return (Class)parameterizedType.getRawType();
      }
    }
    throw new IllegalArgumentException("Type parameter [" + type.toString() + "] not a class or " +
              "parameterized type whose raw type is a class");
  }

  /**
   * Use the typeRef's parameter to create a new instance of the TypeRef's template parameter.
   * @return a new instance of type T
   * @throws NoSuchMethodException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   * @throws InstantiationException
   */
  @SuppressWarnings("unchecked")
  public T newInstance()
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    if (constructor_ == null) {
      constructor_ = getRawClass().getConstructor();
    }

    return (T)constructor_.newInstance();
  }

  /**
   * The cheap, ugly version of the above, for when you don't want to catch 900 exceptions at
   * the calling site.
   * @return a new instance of type T.
   */
  public T safeNewInstance() {
    try {
      return newInstance();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException(e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Gets underlying {@code Type} instance derived from the
   * type.
   * @return the type.
   */
  public final Type getType() {
    return type_;
  }

  /**
   * Gets underlying raw class instance derived from the
   * type.
   * @return the class.
   */
  public final Class<T> getRawClass() {
    if (class_ == null) {
      class_ = getClass(type_);
    }
    return class_;
  }
}
