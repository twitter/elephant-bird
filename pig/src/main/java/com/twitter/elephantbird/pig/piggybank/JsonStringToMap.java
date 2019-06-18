package com.twitter.elephantbird.pig.piggybank;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.google.common.collect.Maps;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

/**
 * Transforms a Json string into a Pig map whose value type is chararray. Only goes one level deep;
 * All input map values are converted to strings via {@link Object#toString()}.
 */
public class JsonStringToMap extends EvalFunc<Map<String, String>> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonStringToMap.class);
  private static final JsonFactory jsonFactory = new JsonFactory();
  private final PigCounterHelper counterHelper = new PigCounterHelper();

  @Override
  public Schema outputSchema(Schema input) {
    try {
      return Utils.getSchemaFromString("json: [chararray]");
    } catch (ParserException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> exec(Tuple input) throws IOException {
    try {
      // Verify the input is valid, logging to a Hadoop counter if not.
      if (input == null || input.size() < 1) {
        throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
      }

      if (input.get(0) == null) {
        counterHelper.incrCounter(getClass().getName(), "NullJsonString", 1L);
        return null;
      }

      String jsonLiteral = (String) input.get(0);
      return parseStringToMap(jsonLiteral);
    } catch (ExecException e) {
      LOG.warn("Error in " + getClass() + " with input " + input, e);
      throw new IOException(e);
    }
  }

  protected Map<String, String> parseStringToMap(String line) throws IOException {
    JsonParser jsonParser = jsonFactory.createParser(line);
    jsonParser.nextToken();
    return walkJsonObject(jsonParser);
  }
  private Map<String, String> walkJsonObject(JsonParser jsonParser) throws IOException {
    Map<String, String> map = Maps.newHashMap();
    for (String key = jsonParser.nextFieldName(); key != null; key = jsonParser.nextFieldName()) {
      String value = jsonToString(jsonParser);
      map.put(key, value);
    }
    return map;
  }

  public String jsonToString(JsonParser jsonParser) throws IOException {
    StringWriter sw = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(sw);
    int level = 0;

    do {
      JsonToken token = jsonParser.nextToken();
      switch (token.id()) {
        case JsonTokenId.ID_START_OBJECT:
          level++;
          jsonGenerator.writeStartObject();
          break;
        case JsonTokenId.ID_END_OBJECT:
          level--;
          jsonGenerator.writeEndObject();
          break;
        case JsonTokenId.ID_START_ARRAY:
          level++;
          jsonGenerator.writeStartArray();
          break;
        case JsonTokenId.ID_END_ARRAY:
          level--;
          jsonGenerator.writeEndArray();
          break;
        case JsonTokenId.ID_FIELD_NAME:
          jsonGenerator.writeFieldName(jsonParser.getText());
          break;
        case JsonTokenId.ID_STRING:
          jsonGenerator.writeString(jsonParser.getText());
          break;
        case JsonTokenId.ID_NUMBER_INT:
          jsonGenerator.writeNumber(jsonParser.getText());
          break;
        case JsonTokenId.ID_NUMBER_FLOAT:
          jsonGenerator.writeNumber(jsonParser.getText());
          break;
        case JsonTokenId.ID_TRUE:
          jsonGenerator.writeBoolean(true);
          break;
        case JsonTokenId.ID_FALSE:
          jsonGenerator.writeBoolean(false);
          break;
        case JsonTokenId.ID_NULL:
          jsonGenerator.writeNull();
          break;
        case JsonTokenId.ID_EMBEDDED_OBJECT:
          jsonGenerator.writeObject(jsonParser.getEmbeddedObject());
          break;
        default:
          throw new IOException("Cno");
      }
    } while (level > 0);
    jsonGenerator.close();
    return sw.toString();
  }
}
