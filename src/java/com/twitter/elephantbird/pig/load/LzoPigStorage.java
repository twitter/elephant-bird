package com.twitter.elephantbird.pig.load;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.builtin.PigStorage;

import com.hadoop.compression.lzo.LzopCodec;
import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

/**
 * 
 * A LZO Loader based on PigStorage Advantages of using this class is that we
 * get all of the improvements that PigStorage have and might get for free.
 * 
 */
public class LzoPigStorage extends PigStorage {

	public LzoPigStorage() {
		super();
	}

	public LzoPigStorage(String delimiter) {
		super(delimiter);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new LzoTextInputFormat();
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		job.getConfiguration().set("mapred.textoutputformat.separator", "");
		FileOutputFormat.setOutputPath(job, new Path(location));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
	}

}
