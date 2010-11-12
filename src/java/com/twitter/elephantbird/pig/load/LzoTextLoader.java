package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapreduce.input.LzoTextInputFormat;

/**
 * Load the LZO file line by line, passing each line as a single-field Tuple to
 * Pig.
 */
public class LzoTextLoader extends LzoBaseLoadFunc {
	private static final Logger LOG = LoggerFactory
			.getLogger(LzoTextLoader.class);

	private static final TupleFactory tupleFactory_ = TupleFactory
			.getInstance();

	protected enum LzoTextLoaderCounters {
		LinesRead
	}

	private ArrayList<Object> mProtoTuple = null;

	public LzoTextLoader() {
	}

	/**
	 * Return every non-null line as a single-element tuple to Pig.
	 */
	@Override
	public Tuple getNext() throws IOException {
		if (reader_ == null) {
			return null;
		}

		mProtoTuple = new ArrayList<Object>();

		Tuple t = null;
		try {

			if (reader_.nextKeyValue()) {

				Text line = (Text) reader_.getCurrentValue();

				if (line != null) {
					incrCounter(LzoTextLoaderCounters.LinesRead, 1L);

					mProtoTuple.add(new DataByteArray(line.getBytes(), 0, line
							.getLength()));

					t = tupleFactory_.newTupleNoCopy(mProtoTuple);

				}
			}
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}
		return t;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new LzoTextInputFormat();
	}

}
