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

/**
 * Wrapper for the schema information we need.
 * Includes an Avro {@link Schema} and information on the mapping of names.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class ParquetSchema {

  @Getter
  private final Schema avroSchema;

  private final Map<String, String> fieldsToSchema = new HashMap<>();

  /**
   * Create a dense {@link ParquetSchema} for the given {@link Collection} of objects.
   *
   * @param clazz the {@link Class} of objects to create the schema for
   * @param objects the {@link Collection} of objects to create the schema for
   * @param name the name of the schema
   * @param <T> the type parameter for the {@link Class} and {@link Collection}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @SuppressWarnings("CyclomaticComplexity")
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
          } else if (long.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredLong(fieldName);
          } else if (int.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredInt(fieldName);
          } else if (double.class.isAssignableFrom(fieldType)) {
            schemaBuilder = schemaBuilder.requiredDouble(fieldName);
          } else if (long[].class.isAssignableFrom(fieldType)
              || BaseTraceObject[].class.isAssignableFrom(fieldType)) {
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

  /**
   * Convert POJO to a {@link GenericRecord} to write it with the {@link org.apache.parquet.avro.AvroParquetWriter}.
   *
   * @param pojo the POJO to convert to a {@link GenericRecord}
   * @param clazz the {@link Class} to which the POJO belongs
   * @param <T> type parameter for the {@link Class} and POJO
   * @return a {@link GenericRecord} containing the POJO
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public <T> GenericRecord convertFromPojo(T pojo, Class<T> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    GenericData.Record record = new GenericData.Record(avroSchema);
    try {
      for (Field field : fields) {
        if (Modifier.isPublic(field.getModifiers()) && fieldsToSchema.containsKey(field.getName())) {
          Object object = field.get(pojo);
          if (object instanceof BaseTraceObject[]) {
            object = Arrays.stream((BaseTraceObject[]) object)
                .map(BaseTraceObject::getId)
                .toArray();
          }
          record.put(fieldsToSchema.get(field.getName()), object);
        }
      }
    } catch (IllegalAccessException e) {
      log.error("Could not convert to Avro record {}.", e.getMessage());
    }
    log.debug("Converted record {}.", record);
    return record;
  }
}
