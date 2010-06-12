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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

public class Invoker<T>  {

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
        } else {
            return klass;
        }
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
            args[i] =  unPrimitivize(paramClasses_[i]).cast(t.get(i));
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






