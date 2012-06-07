package com.twitter.elephantbird.pig.util;

import com.twitter.elephantbird.pig.piggybank.JsonToPigParser;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.*;

import java.io.IOException;

/**
 * Converter for use by the {@link com.twitter.elephantbird.pig.load.SequenceFileLoader}.
 * Delegates to {@link JsonToPigParser} with its defaults (don't create nested maps from nested json, and don't detect
 * json in all string property values).
 *
 * Usage Example:
 * Given a sequence file at /test/foo.seq with Null keys, and Text (json) values similar to:
 * <pre>
 <null> {"a":1,"message":"{\"b\":2}",detail:{"b":2}}
 <null> {"a":3,"message":"{\"b\":3}",detail:{"b":2}}
 <null> {"a":2,"message":"{\"b\":1}",detail:{"b":3}}
 <null> {"a":9,"message":"{\"b\":22}",detail:{"b":7}}
 <null> {"a":15,"message":"{\"b\":25}",detail:{"b":9}}
 <null> {"a":7,"message":"{\"b\":52}",detail:{"b":11}}
 </pre>
 * The following pig script will parse out the "a" values from the top level:
 * <pre>
 json_maps = LOAD '/test/foo.seq' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 '-c com.twitter.elephantbird.pig.util.NullWritableConverter',
 '-c com.jivesoftware.pig.JsonConverter')
 AS (key, json_map);
 aVals = FOREACH json_maps GENERATE json_map#'a' as aVal;
 dump aVals
 -- Output:
 (1)
 (3)
 (2)
 (9)
 (15)
 (7)

 * By default nesting is not enabled, so all child properties will just be the string values:

 detailStrings = FOREACH json_maps GENERATE json_map#'detail';
 dump detailStrings;
 -- Output:
 ({"b":2})
 ({"b":2})
 ({"b":3})
 ({"b":7})
 ({"b":9})
 ({"b":11})

 * Enable nested support with the NESTED_JSON argument to the converter:
 json_maps = LOAD '/test/foo.seq' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 '-c com.twitter.elephantbird.pig.util.NullWritableConverter',
 '-c com.jivesoftware.pig.JsonConverter NESTED_JSON')
 AS (key, json_map);
 details = FOREACH json_maps GENERATE json_map#'detail';
 dump details;
 -- Output:
 [b#2]
 [b#2]
 [b#3]
 [b#7]
 [b#9]
 [b#11]

 * Also, by default, if the json map contains string values that are actually more json encoded data, the converter
 * will not detect that (even with NESTED_JSON enabled):
 json_maps = LOAD '/test/foo.seq' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 '-c com.twitter.elephantbird.pig.util.NullWritableConverter',
 '-c com.jivesoftware.pig.JsonConverter NESTED_JSON')
 AS (key, json_map);
 messages = FOREACH json_maps GENERATE json_map#'message';
 dump messages;
 -- Output:
 {"b":2}
 {"b":3}
 {"b":1}
 {"b":22}
 {"b":25}
 {"b":52}

 * Enable the auto-detection of string values that are themselves json with the DETECT_JSON_IN_ALL_STRINGS argument
 * to the converter:

 json_maps = LOAD '/test/foo.seq' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
 '-c com.twitter.elephantbird.pig.util.NullWritableConverter',
 '-c com.jivesoftware.pig.JsonConverter DETECT_JSON_IN_ALL_STRINGS')
 AS (key, json_map);
 messages = FOREACH json_maps GENERATE json_map#'message';
 dump messages;
 -- Output:
 [b#2]
 [b#3]
 [b#1]
 [b#22]
 [b#25]
 [b#52]
 </pre>
 */
public class JsonConverter extends TextConverter {
    private final JsonToPigParser parser;

    public JsonConverter() {
        this(new JsonToPigParser());
    }

    public JsonConverter(String...features){
        this(new JsonToPigParser(features));
    }

    public JsonConverter(JsonToPigParser.Features...features){
        this(new JsonToPigParser(features));
    }

    public JsonConverter(JsonToPigParser parser) {
        this.parser = parser;
    }

    @Override
    public ResourceSchema.ResourceFieldSchema getLoadSchema() throws IOException {
        ResourceSchema.ResourceFieldSchema schema = new ResourceSchema.ResourceFieldSchema();
        schema.setType(DataType.MAP);
        return schema;
    }


    @Override
    public Object bytesToObject(DataByteArray dataByteArray) throws IOException {
        String json = (String) super.bytesToObject(dataByteArray);
        return parser.toPigMap(json);
    }
}
