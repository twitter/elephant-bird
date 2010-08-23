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
import java.io.Serializable;

import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.Tuple;

/**
 * A grouping of data on that can be processed individually by Pig. Instances of
 * this interface are created by {@link Slicer}, serialized, and sent to nodes
 * to be processed.
 * <p>
 * {@link #getLocations} is called as part of the configuration process to
 * determine where this Slice should be run for maximal locality with the data
 * to be read. Once the Slice arrives on the processing node,
 * {@link #init(DataStorage)} is called to give it access to the
 * <code>DataStorage</code> it should use to load Tuples. After
 * <code>init</code> has been called, any of the other methods on this
 * interface may be called as part of Pig's processing.
 */
public interface Slice extends Serializable {

    /**
     * Returns string representations of all the files that will be used as part
     * of processing this Slice.
     * <p>
     * 
     * This is the only method on Slice that is valid to call before
     * {@link #init(DataStorage)} has been called.
     */
    String[] getLocations();

    /**
     * Initializes this Slice with the DataStorage it's to use to do its work.
     * <p>
     * This will always be called before <code>getLength</code>,
     * <code>close</code>, <code>getPos</code>, <code>getProgress</code>
     * and <code>next</code>.
     */
    void init(DataStorage store) throws IOException;

    /**
     * Returns the offset from which data in this Slice will be processed.
     */
    long getStart();
    
    /**
     * Returns the length in bytes of all of the data that will be processed by
     * this Slice.
     * <p>
     * Only valid to call after {@link #init(DataStorage)} has been called.
     */
    long getLength();

    /**
     * Closes any streams this Slice has opened as part of its work.
     * <p>
     * Only valid to call after {@link #init(DataStorage)} has been called.
     */
    void close() throws IOException;

    /**
     * Returns the number of bytes read so far as part of processing this Slice.
     * <p>
     * Only valid to call after {@link #init(DataStorage)} has been called.
     */
    long getPos() throws IOException;

    /**
     * Returns the percentage of Slice that is complete from 0.0 to 1.0.
     * <p>
     * Only valid to call after {@link #init(DataStorage)} has been called.
     */
    float getProgress() throws IOException;

    /**
     * Loads the next value from this Slice into <code>value</code>.
     * <p>
     * Only valid to call after {@link #init(DataStorage)} has been called.
     * 
     * @param value -
     *                the Tuple to be filled with the next value.
     * @return - true if there are more Tuples to be read.
     */
    boolean next(Tuple value) throws IOException;

}
