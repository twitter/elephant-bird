package com.twitter.elephantbird.pig.piggybank;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Delegates to {@link JsonToPigParser} with its defaults (don't create nested maps from nested json, and don't detect
 * json in all string property values).
 *
 * Usage Example:
 * Given the following json content:
 * <pre>
 {"a":1,"message":"{\"b\":2}",detail:{"b":2}}
 {"a":3,"message":"{\"b\":3}",detail:{"b":2}}
 {"a":2,"message":"{\"b\":1}",detail:{"b":3}}
 {"a":9,"message":"{\"b\":22}",detail:{"b":7}}
 {"a":15,"message":"{\"b\":25}",detail:{"b":9}}
 {"a":7,"message":"{\"b\":52}",detail:{"b":11}}
 </pre>
 * The following pig script will parse out the "a" values from the top level:
 * <pre>
 json_strings = LOAD '/test/foo.json' as (json_string);
 maps = FOREACH json_strings GENERATE com.jivesoftware.pig.JsonToStringMap((chararray)json_string) as json;
 aVals = foreach maps generate json#'a') as aVal;
 dump aVals
 -- Output:
 (1)
 (3)
 (2)
 (9)
 (15)
 (7)
 </pre>
 * Getting the EvalFunc to generate nested maps proved to be an exercise in futility (it seems to work just fine from
 * loaders though:  see {@link com.twitter.elephantbird.pig.util.JsonConverter}.  However, you can get at data under the detail key with:
 <pre>
 maps = FOREACH json_strings GENERATE com.jivesoftware.pig.JsonStringToMap((chararray)json_string) as json;
 details = foreach maps generate com.jivesoftware.pig.JsonStringToMap((chararray)json#'detail') as detail;
 dump details
 -- Output:
 ([b#2])
 ([b#2])
 ([b#3])
 ([b#7])
 ([b#9])
 ([b#11])
 </pre>
 */
public class JsonStringToMap extends EvalFunc<Map> {
    private final JsonToPigParser parser = JsonToPigParser.DEFAULT;

    @Override
    public Map exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() < 1) {
                throw new IOException("Not enough arguments to " + this.getClass().getName() + ": expected at least 1");
            }

            String json = (String) input.get(0);
            Map<String, Object> map = parser.toPigMap(json);
            if(map == null || !parser.isNestedJson()){
                return map;
            }
            Map<String, String> stringMap = new HashMap<String,String>();
            for(Map.Entry<String,Object> entry : map.entrySet()){
                stringMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return stringMap;
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }
}
