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

package com.hirohanin.elephantbird;

import java.io.IOException;

import org.apache.pig.backend.datastorage.DataStorage;

/**
 * Produces independent slices of data from a given location to be processed in
 * parallel by Pig.
 * <p>
 * If a class implementing this interface is given as the LoadFunc in a Pig
 * script, it will be used to make slices for that load statement.
 */
public interface Slicer {
    /**
     * Checks that <code>location</code> is parsable by this Slicer, and that
     * if the DataStorage is used by the Slicer, it's readable from there. If it
     * isn't, an IOException with a message explaining why will be thrown.
     * <p>
     * This does not ensure that all the data in <code>location</code> is
     * valid. It's a preflight check that there's some chance of the Slicer
     * working before actual Slices are created and sent off for processing.
     */
    void validate(DataStorage store, String location) throws IOException;

    /**
     * Creates slices of data from <code>store</code> at <code>location</code>.
     * 
     * @return the Slices to be serialized and sent out to nodes for processing.
     */
    Slice[] slice(DataStorage store, String location) throws IOException;
}

