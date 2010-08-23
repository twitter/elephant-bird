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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.SeekableInputStream;
import org.apache.pig.backend.datastorage.SeekableInputStream.FLAGS;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.SampleLoader;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.tools.bzip2r.CBZip2InputStream;

/**
 * Slice that loads data using a LoadFunc.
 */
public class PigSlice implements Slice {

    public PigSlice(String path, FuncSpec parser, long start, long length) {
        this.file = path;
        this.start = start;
        this.length = length;
        this.parser = parser;
    }

    public FuncSpec getParser() {
        return parser;
    }

    public long getStart() {
        return start;
    }
    
    public long getLength() {
        return length;
    }

    public String[] getLocations() {
        return new String[] { file };
    }

    @SuppressWarnings("unchecked")
	public void init(DataStorage base) throws IOException {
        if (parser == null) {
        	loader = new PigStorage();
        } else {
            try {
                loader = (LoadFunc) PigContext.instantiateFuncFromSpec(parser);
            } catch (Exception exp) {
                int errCode = 2081;
                String msg = "Unable to set up the load function.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }
        
        end = start + getLength();
        
        // Since we are not block aligned and we throw away the first
        // record (count on a different instance to read it), we make the 
        // successor block overlap one byte with the previous block when
        // using PigStorage for uncompressed text files (which in turn 
        // uses Hadoop LineRecordReader).
        if (start != 0 && (loader instanceof PigStorage)) {
            if (!(file.endsWith(".bz") || file.endsWith(".bz2")) &&
                    !(file.endsWith(".gz"))) {
                --start;        
            }
        }
     
        fsis = base.asElement(base.getActiveContainer(), file).sopen();
        fsis.seek(start, FLAGS.SEEK_CUR);

        if (file.endsWith(".bz") || file.endsWith(".bz2")) {
            is = new org.apache.hadoop.io.compress.bzip2.CBZip2InputStream(fsis);
        } else if (file.endsWith(".gz")) {
            is = new GZIPInputStream(fsis);
            // We can't tell how much of the underlying stream GZIPInputStream
            // has actually consumed
            end = Long.MAX_VALUE;
        } else {
            is = fsis;
        }       
           
        // set the right sample size
        if (loader instanceof SampleLoader) {
        	try {
	          	PigContext pc = (PigContext) ObjectSerializer.deserialize(((HDataStorage)base).getConfiguration().getProperty("pig.pigContext"));
	          	ArrayList<Pair<FileSpec, Boolean>> inputs = 
	          		(ArrayList<Pair<FileSpec, Boolean>>) ObjectSerializer.deserialize(((HDataStorage)base).getConfiguration().getProperty("pig.inputs"));
	          	
	          	((SampleLoader)loader).computeSamples(inputs, pc);	          	
	        } catch (Exception e) {
	           	throw new ExecException(e.getMessage());
	        }
    	}        
        
     
    }
    
    public boolean next(Tuple value) throws IOException {
        Tuple t = loader.getNext();
        if (t == null) {
            return false;
        }
        value.reference(t);
        return true;
    }
    
    public long getPos() throws IOException {
        return fsis.tell();
    }

    public void close() throws IOException {
        is.close();
    }

    public float getProgress() throws IOException {
        float progress = getPos() - start;
        float finish = getLength();
        return progress / finish;
    }
    
    public void readFields(DataInput is) throws IOException {
        file = is.readUTF();
        start = is.readLong();
        length = is.readLong();
        parser = (FuncSpec)this.readObject(is);
    }

    public void write(DataOutput os) throws IOException {
        os.writeUTF(file);
        os.writeLong(start);
        os.writeLong(length);
        this.writeObject(parser, os);
    }
    
    private void writeObject(Serializable obj, DataOutput os)
    throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        byte[] bytes = baos.toByteArray();
        os.writeInt(bytes.length);
        os.write(bytes);
    }

    private Object readObject(DataInput is) throws IOException {
        byte[] bytes = new byte[is.readInt()];
        is.readFully(bytes);
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(
                bytes));
        try {
            return ois.readObject();
        } catch (ClassNotFoundException cnfe) {
            int errCode = 2094;
            String msg = "Unable to deserialize object.";
            throw new ExecException(msg, errCode, PigException.BUG, cnfe);
        }
    }

    
    // assigned during construction
    String file;
    long start;
    long length;
    FuncSpec parser;

    // Created as part of init
    transient private InputStream is;
    transient private SeekableInputStream fsis;
    private long end;
    transient private LoadFunc loader;

    private static final long serialVersionUID = 1L;
}
