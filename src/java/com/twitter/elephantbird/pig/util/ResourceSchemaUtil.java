package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.List;

import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;

/**
 * Utilities for {@link ResourceSchema} and friends.
 *
 * @author Andy Schlaikjer
 */
public final class ResourceSchemaUtil {
  /**
   * Creates a new ResourceFieldSchema which reflects data from an input RequiredField.
   *
   * @param field
   * @return new ResourceFieldSchema which reflects {@code field}.
   * @throws IOException
   */
  public static ResourceFieldSchema createResourceFieldSchema(RequiredField field)
      throws IOException {
    ResourceFieldSchema schema =
        new ResourceFieldSchema().setName(field.getAlias()).setType(field.getType());
    List<RequiredField> subFields = field.getSubFields();
    if (subFields != null && !subFields.isEmpty()) {
      ResourceFieldSchema[] subSchemaFields = new ResourceFieldSchema[subFields.size()];
      int i = 0;
      for (RequiredField subField : subFields) {
        subSchemaFields[i++] = createResourceFieldSchema(subField);
      }
      ResourceSchema subSchema = new ResourceSchema();
      subSchema.setFields(subSchemaFields);
      schema.setSchema(subSchema);
    }
    return schema;
  }

  private ResourceSchemaUtil() {
    // hide ctor
  }
}
