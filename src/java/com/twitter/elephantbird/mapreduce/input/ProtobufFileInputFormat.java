package com.twitter.elephantbird.mapreduce.input;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
import com.twitter.elephantbird.mapreduce.io.ProtobufConverter;
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.StreamSearcher;
import com.twitter.elephantbird.util.TypeRef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * FileInputFormat for processing uncompressed protocol buffers. That is, if you write your data
 * in the form of [{@link Protobufs.KNOWN_GOOD_POSITION_MARKER} writeDelimited PB] with no compression, then this
 * format can read it.
 *
 * Do not use ProtobufFileInputFormat.class directly for setting InputFormat class for a job.
 * use getInputFormatClass() instead.
 */
public class ProtobufFileInputFormat<M extends Message> extends FileInputFormat<LongWritable,ProtobufWritable<M>> {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufConverter.class);
    private TypeRef<M> typeRef_;

    public ProtobufFileInputFormat() {
        super();
    }

    public ProtobufFileInputFormat(TypeRef<M> typeRef_) {
        super();
        this.typeRef_ = typeRef_;
    }

    protected void setTypeRef(TypeRef<M> typeRef){
        typeRef_ = typeRef;
    }

    /**
     * Returns {@link ProtobufFileInputFormat} class.
     * Sets an internal configuration in jobConf so that remote Tasks
     * instantiate appropriate object based on protoClass.
     */
    @SuppressWarnings("unchecked")
    public static <M extends Message> Class<ProtobufFileInputFormat>
    getInputFormatClass(Class<M> protoClass, Configuration jobConf) {
        Protobufs.setClassConf(jobConf, LzoProtobufBlockInputFormat.class, protoClass);
        return ProtobufFileInputFormat.class;
    }

    public static<M extends Message> ProtobufFileInputFormat<M> newInstance(TypeRef<M> typeRef) {
        return new ProtobufFileInputFormat<M>(typeRef);
    }

    @Override
    public RecordReader<LongWritable, ProtobufWritable<M>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(typeRef_==null){
            typeRef_ = Protobufs.getTypeRef(taskAttemptContext.getConfiguration(),ProtobufFileInputFormat.class);
        }
        return new ProtobufFileRecordReader<M>(typeRef_);
    }

    private static class ProtobufFileRecordReader<M extends Message> extends RecordReader<LongWritable, ProtobufWritable<M>>{
        private final TypeRef<M> typeRef_;
        private LongWritable key_;
        private ProtobufWritable<M> value_;
        private long start_;
        private long end_;
        private long pos_;
        private FSDataInputStream fileIn_;
        private Message.Builder protoBuilder;


        private ProtobufFileRecordReader(TypeRef<M> typeRef_) {
            this.typeRef_=typeRef_;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Configuration job = taskAttemptContext.getConfiguration();
            FileSplit split = (FileSplit)inputSplit;

            start_ = split.getStart();
            end_ = split.getLength()+start_;

            final Path file = split.getPath();

            FileSystem fs = file.getFileSystem(job);
            fileIn_ = fs.open(file);
            fileIn_.seek(start_);

            StreamSearcher searcher = new StreamSearcher(Protobufs .KNOWN_GOOD_POSITION_MARKER);

            if(start_!=0){
                fileIn_.seek(start_);
                searcher.search(fileIn_);
            }

            pos_ = fileIn_.getPos();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(pos_ > end_||fileIn_.available()<=0){
                //we are done!
                return false;
            }
            if(key_==null)
                key_ = new LongWritable();
            key_.set(pos_);

            //skip the position marger
            if(Protobufs.KNOWN_GOOD_POSITION_MARKER.length+pos_<=end_)
                fileIn_.skipBytes(Protobufs.KNOWN_GOOD_POSITION_MARKER.length);

            pos_ = fileIn_.getPos();

            try {
                if (protoBuilder == null) {
                    protoBuilder = Protobufs.getMessageBuilder(typeRef_.getRawClass());
                }
                if(value_==null)
                    value_ = new ProtobufWritable<M>(typeRef_);

                Message.Builder builder = protoBuilder.clone();
                final boolean success = builder.mergeDelimitedFrom(fileIn_);
                if(success){
                    value_.set((M) builder.build());
                    LOG.trace(value_.get().toString());
                }
                return success;
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Invalid Protobuf exception while building " + typeRef_.getRawClass().getName(), e);
            } catch(UninitializedMessageException ume) {
                LOG.error("Uninitialized Message Exception while building " + typeRef_.getRawClass().getName(), ume);
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key_;

        }

        @Override
        public ProtobufWritable<M> getCurrentValue() throws IOException, InterruptedException {
            return value_;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float)(pos_-start_)/(float)(end_-start_);
        }

        @Override
        public void close() throws IOException {
            fileIn_.close();
        }
    }
}
