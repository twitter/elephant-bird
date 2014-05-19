package com.twitter.elephantbird.mapreduce.input;

import com.twitter.elephantbird.mapreduce.input.combined.DelegateCombineFileInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LzoCombineFileInputFormat extends DelegateCombineFileInputFormat<LongWritable, Text> {
    public LzoCombineFileInputFormat() {
        super(new LzoTextInputFormat());
    }
}
