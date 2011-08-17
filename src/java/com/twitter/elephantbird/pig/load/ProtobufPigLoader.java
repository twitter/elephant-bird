package com.twitter.elephantbird.pig.load;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.ProtobufFileInputFormat;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/15/11
 *         Time: 11:17 AM
 */
public class ProtobufPigLoader<M extends Message> extends LoadFunc {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufPigLoader.class);
    private TypeRef<M> typeRef_ = null;

    private RecordReader reader_;
    /**
     * Default constructor. Do not use for actual loading.
     */
    public ProtobufPigLoader() {}

    public ProtobufPigLoader(TypeRef<M> typeRef_) {
        this.typeRef_ = typeRef_;
    }

    public ProtobufPigLoader(String protoClassname) {
        this.typeRef_ = PigUtil.getProtobufTypeRef(protoClassname);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new ProtobufFileInputFormat<M>(typeRef_);
    }


    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        reader_ = recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if(reader_ ==null)
                LOG.warn("Reader is null!");
            else if(reader_.nextKeyValue()){
                M value = ((ProtobufWritable<M>)reader_.getCurrentValue()).get();

                return value!=null? new ProtobufTuple(value):null;
            }
        } catch (InterruptedException e) {
            LOG.error("InterruptedException encountered, bailing.", e);
            throw new IOException(e);
        }
        return null; //should never happen
    }
}
