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

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
/**
 * The generic Invoker class does all the common grunt work of setting up an invoker.
 * Class-specific non-generic extensions of this class are needed for Pig to know what type
 * of return to expect from exec, and to find the appropriate classes through reflection.
 * All they have to do is implement the constructors that call into super(). Note that the 
 * no-parameter constructor is <b>required</b>, if seemingly nonsensical, for Pig to do its work.
 * <p>
 * The Invoker family of udfs understand the following class names (all case-independent):
 * <li>String
 * <li>Long
 * <li>Float
 * <li>Double
 * <li>Int
 * <p>
 * Invokers can also work with array arguments, represented in Pig as DataBags of single-tuple
 * elements. Simply refer to <code>string[]</code>, for example.
 * <p>
 * This UDF allows one to dynamically invoke Java methods that return a <code>T</code>
 * <p>
 * Usage of the Invoker family of UDFs (adjust as appropriate):
 * <p>
 *<pre>
 * {@code
 * -- invoking a static method
 * DEFINE StringToLong InvokeForLong('java.lang.Long.valueOf', 'String')
 * longs = FOREACH strings GENERATE StringToLong(some_chararray);
 *
 * -- invoking a method on an object
 * DEFINE StringConcat InvokeForString('java.lang.String.concat', 'String String', 'false')
 * concatenations = FOREACH strings GENERATE StringConcat(str1, str2); 
 * }
 * </pre>
 * <p>
 * The first argument to the constructor is the full path to desired method.<br>
 * The second argument is a list of classes of the method parameters.<br>
 * If the method is not static, the first element in this list is the object to invoke the method on.<br>
 * The second argument is optional (a no-argument static method is assumed if it is not supplied).<br>
 * The third argument is the keyword "static" (or "true") to signify that the method is static. <br>
 * The third argument is optional, and true by default.<br>
 * <p>
 * @param <T>
 */
public abstract class GenericInvoker<T> extends EvalFunc<T> {

    private Invoker<T> invoker_;

    public GenericInvoker() {}

    public GenericInvoker(String fullName) 
    throws ClassNotFoundException, FrontendException, SecurityException, NoSuchMethodException {
      invoker_ = new Invoker<T>(fullName, "");
    }

    public GenericInvoker(String fullName, String paramSpecsStr)
    throws ClassNotFoundException, FrontendException, SecurityException, NoSuchMethodException {
        invoker_ = new Invoker<T>(fullName, paramSpecsStr);
    }

    public GenericInvoker(String fullName, String paramSpecsStr, String isStatic)
    throws ClassNotFoundException, FrontendException, SecurityException, NoSuchMethodException {
        invoker_ = new Invoker<T>(fullName, paramSpecsStr, isStatic);
    }

    @Override
    public T exec(Tuple input) throws IOException {
        if (invoker_ == null) {
            throw new ExecException("exec() attempted on an unitialized invoker. " +
            "Invokers must be constructed with the method to invoke, and parameter signature to same.");
        }
        return invoker_.invoke(input);
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (invoker_ == null) return null;
        FieldSchema fs = new FieldSchema(null, DataType.findType(invoker_.getReturnType()));
        return new Schema(fs);
    }
}
