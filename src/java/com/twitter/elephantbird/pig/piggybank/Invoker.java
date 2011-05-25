/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.elephantbird.pig.piggybank;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class Invoker<T>  {

    private static final Log LOG = LogFactory.getLog(Invoker.class);

    private static final Class<?> DOUBLE_ARRAY_CLASS = new double[0].getClass();
    private static final Class<?> INT_ARRAY_CLASS = new int[0].getClass();
    private static final Class<?> FLOAT_ARRAY_CLASS = new float[0].getClass();
    private static final Class<?> STRING_ARRAY_CLASS = new String[0].getClass();
    private static final Class<?> LONG_ARRAY_CLASS = new long[0].getClass();

    @SuppressWarnings("unchecked")
    private static final Set<Class<?>> ARRAY_CLASSES = Sets.newHashSet(
        DOUBLE_ARRAY_CLASS, INT_ARRAY_CLASS, FLOAT_ARRAY_CLASS, STRING_ARRAY_CLASS,
        LONG_ARRAY_CLASS);

    private Method method_;
    private Class<?>[] paramClasses_;

    private boolean isStatic_;
    private Class<?> selfClass_;
    private Type returnType_;

    public Invoker(String fullName, String paramSpecsStr)
    throws ClassNotFoundException, FrontendException, SecurityException, NoSuchMethodException {
        this(fullName, paramSpecsStr, "true");
    }

    public Invoker(String fullName, String paramSpecsStr, String isStatic)
    throws ClassNotFoundException, FrontendException, SecurityException, NoSuchMethodException {
        String className = fullName.substring(0, fullName.lastIndexOf('.'));
        String methodName = fullName.substring(fullName.lastIndexOf('.')+1);
        Class<?> klazz = Class.forName(className);
        String[] paramSpecs = "".equals(paramSpecsStr) ? new String[0] : paramSpecsStr.split(" ");
        isStatic_ = "static".equalsIgnoreCase(isStatic) || "true".equals(isStatic);
        paramClasses_ = new Class<?>[paramSpecs.length];
        for (int i = 0; i < paramSpecs.length; i++) {
            paramClasses_[i] = stringToClass(paramSpecs[i]);
        }
        if (!isStatic_) {
            selfClass_ = paramClasses_[0];
        }
        method_ = klazz.getMethod(methodName, (isStatic_ ? paramClasses_ : dropFirstClass(paramClasses_)));
        returnType_ = method_.getGenericReturnType();
    }

    @SuppressWarnings("rawtypes")
    public Type getReturnType() {
        return unPrimitivize((Class) returnType_);
    }

    private static Class<?>[] dropFirstClass(Class<?>[] original) {
        if (original.length < 2) {
            return new Class[0];
        } else {
            return Arrays.copyOfRange(original, 1, original.length-1);
        }
    }

    private static Object[] dropFirstObject(Object[] original) {
        if (original.length < 2) {
            return new Object[0];
        } else {
            return Arrays.copyOfRange(original, 1, original.length-1);
        }
    }

    private static Class<?> stringToClass(String klass) throws FrontendException {
        if ("string".equalsIgnoreCase(klass)) {
            return String.class;
        } else if ("int".equalsIgnoreCase(klass)) {
            return Integer.TYPE;
        } else if ("double".equalsIgnoreCase(klass)) {
            return Double.TYPE;
        } else if ("float".equalsIgnoreCase(klass)){
            return Float.TYPE;
        } else if ("long".equalsIgnoreCase(klass)) {
            return Long.TYPE;
        } else if ("double[]".equalsIgnoreCase(klass)) {
          return DOUBLE_ARRAY_CLASS;
        } else if ("int[]".equalsIgnoreCase(klass)) {
          return INT_ARRAY_CLASS;
        } else if ("long[]".equalsIgnoreCase(klass)) {
          return LONG_ARRAY_CLASS;
        } else if ("float[]".equalsIgnoreCase(klass)) {
          return FLOAT_ARRAY_CLASS;
        } else if ("string[]".equalsIgnoreCase(klass)) {
          return STRING_ARRAY_CLASS;
        } else {
            throw new FrontendException("unable to find matching class for " + klass);
        }

    }

    private static Class<?> unPrimitivize(Class<?> klass) {
        if (klass.equals(Integer.TYPE)) {
            return Integer.class;
        } if (klass.equals(Long.TYPE)) {
            return Long.class;
        } else if (klass.equals(Float.TYPE)) {
            return Float.class;
        } else if (klass.equals(Double.TYPE)) {
            return Double.class;
        } else if (klass.equals(DOUBLE_ARRAY_CLASS)) {
            return DOUBLE_ARRAY_CLASS;
        } else {
          return klass;
        }
    }

    private static <T> T convertToExpectedArg(Class<T> klass, Object obj) throws ExecException {
      if (ARRAY_CLASSES.contains(klass)) {
        DataBag dbag = (DataBag) obj;
        if (STRING_ARRAY_CLASS.equals(klass)) {
          List<String> dataList = Lists.newArrayList();
          for (Tuple t : dbag) {
            dataList.add( (String) t.get(0));
          }
          String[] dataArray = new String[dataList.size()];
          for (int i = 0; i < dataList.size(); i++) {
            dataArray[i] = dataList.get(i);
          }
          obj = dataArray;
        } else {
          List<Number> dataList = bagToNumberList(dbag);
          if (DOUBLE_ARRAY_CLASS.equals(klass)) {
            double[] dataArray = new double[dataList.size()];
            for (int i = 0; i < dataList.size(); i++) {
              dataArray[i] = dataList.get(i).doubleValue();
            }
            obj = dataArray;
          } else if (INT_ARRAY_CLASS.equals(klass)) {
            int[] dataArray = new int[dataList.size()];
            for (int i = 0; i < dataList.size(); i++) {
              dataArray[i] = dataList.get(i).intValue();
            }
            obj = dataArray;
          } else if (FLOAT_ARRAY_CLASS.equals(klass)) {
            float[] dataArray = new float[dataList.size()];
            for (int i = 0; i < dataList.size(); i++) {
              dataArray[i] = dataList.get(i).floatValue();
            }
            obj = dataArray;
          } else if (LONG_ARRAY_CLASS.equals(klass)) {
            long[] dataArray = new long[dataList.size()];
            for (int i = 0; i < dataList.size(); i++) {
              dataArray[i] = dataList.get(i).longValue();
            }
            obj = dataArray;
          }
        }
      }
      try {
        return klass.cast(obj);
      } catch (ClassCastException e) {
        LOG.error("Error in dynamic argument processing. Casting to: "
            + klass + " from: " + obj.getClass(), e);
        throw new ExecException(e);
      }
    }

    private static List<Number> bagToNumberList(DataBag dbag) throws ExecException {
      List<Number> dataList = Lists.newArrayList();
      for (Tuple t : dbag) {
        dataList.add( (Number) t.get(0));
      }
      return dataList;
    }

    private Object[] tupleToArgs(Tuple t) throws ExecException {
      if ( (t == null && (paramClasses_ != null || paramClasses_.length != 0))
            || (t != null && t.size() < paramClasses_.length)) {
            throw new ExecException("unable to match function arguments to declared signature.");
        }
        if (t == null) {
            return null;
        }
        Object[] args = new Object[paramClasses_.length];
        for (int i = 0; i < paramClasses_.length; i++) {
            args[i] =  convertToExpectedArg(unPrimitivize(paramClasses_[i]), t.get(i));
        }
        return args;
    }

    @SuppressWarnings("unchecked")
    public T invoke(Tuple input) throws IOException {
        Object[] args = tupleToArgs(input);
        try {
            if (!isStatic_) {
                return (T) method_.invoke(selfClass_.cast(args[0]), dropFirstObject(args));
            } else {
                return (T) method_.invoke(null, args);
            }
        } catch (IllegalArgumentException e) {
            throw new ExecException(e);
        } catch (IllegalAccessException e) {
            throw new ExecException(e);
        } catch (InvocationTargetException e) {
            throw new ExecException(e);
        }
    }

}






