package com.twitter.elephantbird.pig.piggybank;

import com.twitter.elephantbird.pig.util.PigCounterHelper;
import org.apache.pig.data.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Transforms a Json string into a Pig map.  Supports nested json, but only goes one level deep by default.
 * When enabled, nested json will be transformed into a nested Pig map structure with the value types being map (for
 * json objects and maps), bag (for json lists), and charrarray (for everything else).  When
 * nesting is not enabled, all input map values are converted to Strings via {@link Object#toString()} and the
 * resulting pig map value type will be charrarray.
 *
 * Also supports auto-detecting string values that actually contain a json parsable value within an outer json
 * structure (disabled by default).  This is convenient for cases where data has already encoded json values as
 * string fields in an outer data container.  Enabling this feature implies enabling NESTED_JSON support.
 *
 * see {@link JsonStringToMap} for example usages.
 */
public class JsonToPigParser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToPigParser.class);
    public static final JsonToPigParser DEFAULT = new JsonToPigParser();

    private enum JsonToPigParserCounters {
        TotalCalls,
        JsonDecoded,
        ParseError,
        NullJsonString,
        EmbeddedJsonString
    }

    public enum Features {
        NESTED_JSON,
        DETECT_JSON_IN_ALL_STRINGS
    }

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();
    private static final BagFactory bagFactory = DefaultBagFactory.getInstance();

    private final PigCounterHelper counterHelper = new PigCounterHelper();

    private boolean detectJsonInAllStrings;
    private boolean nestedJson;

    public JsonToPigParser() {
        this((Features[])null);
    }

    public JsonToPigParser(String... features){
        this(parseFeatures(features));
    }

    public JsonToPigParser(Features...features){
        if(features != null){
            for(Features feature : features){
                switch(feature){
                    case DETECT_JSON_IN_ALL_STRINGS:
                        detectJsonInAllStrings = true;
                        break;
                    case NESTED_JSON:
                        nestedJson = true;
                        break;
                }
            }
        }
        if(this.detectJsonInAllStrings && !this.nestedJson){
            LOG.warn("The NESTED_JSON feature must be enabled when DETECT_JSON_IN_ALL_STRINGS is enabled.");
            this.nestedJson = true;
        }
    }

    public Map<String,Object> toPigMap(String json){
        counterHelper.incrCounter(JsonToPigParserCounters.TotalCalls, 1);
        if(json == null || "null".equals(json)){
            counterHelper.incrCounter(JsonToPigParserCounters.NullJsonString, 1);
            return null;
        }
        try {
            JsonNode jsonNode = mapper.readTree(json);
            counterHelper.incrCounter(JsonToPigParserCounters.JsonDecoded, 1);
            if(jsonNode instanceof TextNode){
                try{
                    jsonNode = mapper.readTree(jsonNode.getTextValue());
                }catch(Exception ex){
                    //node doesn't contain json, just return the string as normal
                }
            }

            if(jsonNode instanceof ObjectNode){
                return walkJson((ObjectNode) jsonNode);
            }

            Map<String,Object> v = new HashMap<String, Object>();
            Object value;
            if(jsonNode instanceof ArrayNode){
                value = walkJson((ArrayNode)jsonNode);
            }else{
                value = wrap(jsonNode);
            }
            v.put("value", value);
            return v;
        }catch(Exception ex){
            LOG.warn("Could not json-decode string: {}, {}", new Object[]{json, ex.getMessage(), ex});
            counterHelper.incrCounter(JsonToPigParserCounters.ParseError, 1);
            return null;
        }
    }

    public boolean isNestedJson() {
        return nestedJson;
    }

    public boolean isDetectJsonInAllStrings() {
        return detectJsonInAllStrings;
    }

    protected Map<String,Object> walkJson(ObjectNode jsonObj) {
        Map<String,Object> v = new HashMap<String, Object>();
        Iterator<Map.Entry<String,JsonNode>> fields = jsonObj.getFields();
        while(fields.hasNext()){
            Map.Entry<String, JsonNode> field = fields.next();
            v.put(field.getKey(), wrap(field.getValue()));
        }
        return v;
    }

    protected Object walkJson(ArrayNode value) {
        DataBag mapValue = bagFactory.newDefaultBag();
        for (int i=0; i<value.size(); i++) {
            Tuple t = tupleFactory.newTuple(wrap(value.get(i)));
            mapValue.add(t);
        }
        return mapValue;
    }

    protected Object wrap(JsonNode value) {
        if(value == null || value.isNull()){
            return null;
        }
        if(nestedJson){
            if (value instanceof ObjectNode) {
                return walkJson((ObjectNode) value);
            } else if (value instanceof ArrayNode) {
                return walkJson((ArrayNode) value);
            }else if (detectJsonInAllStrings && value instanceof TextNode){
                try{
                    //attempt to parse the json to 'detect' json embedded string values
                    Object wrapped = wrap(mapper.readTree(value.getTextValue()));
                    counterHelper.incrCounter(JsonToPigParserCounters.EmbeddedJsonString, 1);
                    return wrapped;
                }catch(Exception ex){
                    //totally fine, the string value doesn't contain json, it's just a string
                }
            }
        }
        if(value instanceof TextNode){
            return value.getValueAsText();
        }
        return value.toString();
    }

    private static Features[] parseFeatures(String[] featureStrs) {
        Features[] features = new Features[featureStrs.length];
        for(int i=0;i<features.length;i++){
            features[i] = Features.valueOf(featureStrs[i].toUpperCase());
        }
        return features;
    }
}
