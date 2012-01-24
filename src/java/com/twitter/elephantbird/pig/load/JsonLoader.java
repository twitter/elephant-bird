package com.twitter.elephantbird.pig.load;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.pig.load.LzoJsonLoader.LzoJsonLoaderCounters;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * A basic Json Loader. Totally subject to change, this is mostly a cut and paste job.
 */
public class JsonLoader extends PigStorage {

	private static final Logger LOG = LoggerFactory.getLogger(JsonLoader.class);
	@SuppressWarnings("rawtypes")
	protected RecordReader reader_;

	private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
	private static final BagFactory bagFactory_ = DefaultBagFactory.getInstance();

	private final JSONParser jsonParser_ = new JSONParser();

	protected enum JsonLoaderCounters { LinesRead, LinesJsonDecoded, LinesParseError, LinesParseErrorBadNumber }

	// Making accessing Hadoop counters from Pig slightly more convenient.
	private final PigCounterHelper counterHelper_ = new PigCounterHelper();

	private String[]    fields  = null;
	private Set<String> fieldsS = null;

	public JsonLoader() {}

	public JsonLoader(String fields_spec) {
		fields = fields_spec.split(",");
		fieldsS = new HashSet<String>();
		for(String s: fields) {
			fieldsS.add(s);
		}
	}

	/**
	 * Return every non-null line as a single-element tuple to Pig.
	 */
	@Override
	public Tuple getNext() throws IOException {
		if (reader_ == null) {
			return null;
		}
		try {
			while (reader_.nextKeyValue()) {
				Text value_ = (Text)reader_.getCurrentValue();
				incrCounter(LzoJsonLoaderCounters.LinesRead, 1L);
				Tuple t = parseStringToTuple(value_.toString());
				if (t != null) {
					incrCounter(LzoJsonLoaderCounters.LinesJsonDecoded, 1L);
					return t;
				}
			}
			return null;

		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}
	}

	@Override
	public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
		this.reader_ = reader;
	}

	// We could implement JsonInputFormat and proxy to it, and maybe that'd be worthwhile,
	// but taking the cheap shortcut for now.
	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() {
		return new TextInputFormat();
	}

	/**
	 * A convenience function for working with Hadoop counter objects from load functions.  The Hadoop
	 * reporter object isn't always set up at first, so this class provides brief buffering to ensure
	 * that counters are always recorded.
	 */
	protected void incrCounter(Enum<?> key, long incr) {
		counterHelper_.incrCounter(key, incr);
	}

	private boolean isScalar(Object o) {
		return o instanceof Integer
				|| o instanceof Long
				|| o instanceof Float
				|| o instanceof Double
				|| o instanceof String
				|| o instanceof Boolean;
	}

	private DataBag jsonArrayToDataBag(JSONArray jsa) {
		/* Bags, the only way we have to represent Arrays, always contain
		 * Tuples, never scalars */
		DataBag db = bagFactory_.newDefaultBag();
		for(Object dbo: jsa) {
			db.add(jsonThingToTuple(dbo));
		}
		return db;
	}

	private Object jsonScalarToScalar(Object o) {
		/* we don't do anything special here, but we could use this spot to map
		 * String->byte[] if that ever comes up */
		return o;
	}

	private Tuple jsonThingToTuple(Object o) {
		if(o == null) {
			return null;
		}

		if(o instanceof JSONArray) {
			JSONArray jsa = (JSONArray)o;
			return tupleFactory_.newTuple(jsonArrayToDataBag(jsa));
		} else if(o instanceof JSONObject) {
			Map<Object, Object> jso = jsonObjToMap((JSONObject)o);
			return tupleFactory_.newTuple(jso);
		} else if(isScalar(o)) {
			return tupleFactory_.newTuple(jsonScalarToScalar(o));
		}

		throw new RuntimeException("Oh god oh god " + o.toString());
	}

	private Map<Object, Object> jsonObjToMap(JSONObject jso) {
		return jsonObjToMap(jso, false);
	}

	private Map<Object, Object> jsonObjToMap(JSONObject jso, boolean toplevel) {
		Map<Object, Object> ret = new HashMap<Object, Object>(jso.size());

		for(Object k: jso.keySet()) {
			if((toplevel && fields != null)
					&& (!(k instanceof String))
					|| !fieldsS.contains((String)k)) {
				/* if we don't want this field anyway, don't bother reserialising it */
				continue;
			}

			Object val = jso.get(k);
			Object jsval;
			if(isScalar(val)) {
				/* no intermediate tuple around these */
				jsval = jsonScalarToScalar(val);
			} else if(val instanceof JSONArray) {
				/* no intermediate tuple around these */
				jsval = jsonArrayToDataBag((JSONArray)val);
			} else {
				jsval = jsonThingToTuple(val);
			}
			/* we probably only support String keys, but we can try to
			 * support any scalars if pig can handle them */
			ret.put(jsonScalarToScalar(k), jsval);
		}
		return ret;
	}
	protected Tuple parseStringToTuple(String line) {
		try {
			JSONObject jsonObj = (JSONObject)jsonParser_.parse(line);
			Map<Object, Object> m = jsonObjToMap(jsonObj);

			if(fields == null) {
				return tupleFactory_.newTuple(m);
			}

			Tuple ret = tupleFactory_.newTuple();

			for(String f: fields) {
				/* returns null when not present */
				ret.append(m.get(f));
			}
			return ret;
		} catch (ParseException e) {
			LOG.warn("Could not json-decode string: " + line, e);
			incrCounter(JsonLoaderCounters.LinesParseError, 1L);
			return null;
		} catch (NumberFormatException e) {
			LOG.warn("Very big number exceeds the scale of long: " + line, e);
			incrCounter(JsonLoaderCounters.LinesParseErrorBadNumber, 1L);
			return null;
		} catch (ClassCastException e) {
			LOG.warn("Could not convert to Json Object: " + line, e);
			incrCounter(JsonLoaderCounters.LinesParseError, 1L);
			return null;
		}
	}

}
