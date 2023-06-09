package com.asml.apa.wta.core.io;

import com.asml.apa.wta.core.model.BaseTraceObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class ParquetSchema {

  @Getter
  private final Schema avroSchema;

  private final Map<String, String> fieldsToSchema = new HashMap<>();

  public <T> ParquetSchema(Class<T> clazz, Collection<T> objects, String name) {
    String regex = "([a-z])([A-Z]+)";
    String replacement = "$1_$2";
    Field[] fields = clazz.getDeclaredFields();
    SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record(name)
        .namespace("com.asml.apa.wta.core.model")
        .fields();
    try {
      for (Field field : fields) {
        boolean sparseField = false;
        for (T o : objects) {
          if (!Modifier.isPublic(field.getModifiers()) || field.get(o) == null) {
            sparseField = true;
            break;
          }
        }
        if (!sparseField) {
          Class<?> fieldType = field.getType();
          String fieldName =
              field.getName().replaceAll(regex, replacement).toLowerCase();
          if (String.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredString(fieldName);
          } else if (long.class.isAssignableFrom(fieldType) || Long.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredLong(fieldName);
          } else if (int.class.isAssignableFrom(fieldType) || Integer.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredInt(fieldName);
          } else if (double.class.isAssignableFrom(fieldType) || Double.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredDouble(fieldName);
          } else if (float.class.isAssignableFrom(fieldType) || Float.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredFloat(fieldName);
          } else if (boolean.class.isAssignableFrom(fieldType) || Boolean.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredBoolean(fieldName);
          } else if (long[].class.isAssignableFrom(fieldType) || Long[].class.isAssignableFrom(fieldType) || BaseTraceObject[].class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder
                .name(fieldName)
                .type()
                .array()
                .items()
                .longType()
                .noDefault();
          } else {
            log.error("Could not create a valid encoding for {}.", fieldType);
            throw new IllegalAccessException(fieldType.toString());
          }
          fieldsToSchema.put(field.getName(), fieldName);
        }
      }
      avroSchema = schemaBuilder.endRecord();
    } catch (IllegalAccessException e) {
      log.error("Could not create a valid schema for {} in {}.", e.getMessage(), clazz);
      throw new RuntimeException("Could not create a valid schema for " + clazz, e);
    }
  }

  public <T> GenericRecord convertFromPojo(T pojo, Class<T> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    GenericData.Record record = new GenericData.Record(avroSchema);
    try {
      for (Field field : fields) {
        if (Modifier.isPublic(field.getModifiers()) && fieldsToSchema.containsKey(field.getName())) {
          log.info(field.getName());
          Object o = field.get(pojo);
          if (o instanceof BaseTraceObject[]) {
            o = Arrays.stream((BaseTraceObject[]) o).map(BaseTraceObject::getId).toArray();
          }
          record.put(fieldsToSchema.get(field.getName()), o);
        }
      }
    } catch (IllegalAccessException e) {
      log.error("Could not convert to Avro record {}.", e.getMessage());
    }
    return record;
  }
}
