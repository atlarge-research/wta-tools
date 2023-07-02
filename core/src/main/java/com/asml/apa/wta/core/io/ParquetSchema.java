package com.asml.apa.wta.core.io;

import com.asml.apa.wta.core.model.BaseTraceObject;
import com.asml.apa.wta.core.model.Domain;
import com.asml.apa.wta.core.stream.Stream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
   * Create a dense {@link ParquetSchema} for the given {@link Stream} of objects.
   *
   * @param clazz       {@link Class} of objects to create the schema for
   * @param objects     {@link Stream} of objects to create the schema for
   * @param name        name of the schema
   * @param <T>         type parameter for the {@link Class} and {@link Stream}
   */
  @SuppressWarnings("CyclomaticComplexity")
  public <T extends BaseTraceObject> ParquetSchema(Class<T> clazz, Stream<T> objects, String name) {
    String followedByCapitalized = "([a-z0-9])([A-Z]+)";
    String followedByDigit = "([a-zA-Z])([0-9]+)";
    String replacement = "$1_$2";
    Field[] fields = clazz.getDeclaredFields();
    SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record(name)
        .namespace("com.asml.apa.wta.core.model")
        .fields();
    try {
      List<Field> nonStaticValidFields = new ArrayList<>();
      Map<Field, MethodHandle> fieldHandles = new HashMap<>();
      MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
      for (Field field : fields) {
        if (!Modifier.isStatic(field.getModifiers())) {
          nonStaticValidFields.add(field);
        }
        fieldHandles.put(field, lookup.unreflectGetter(field));
      }
      while (!objects.isEmpty()) {
        T object = objects.head();
        List<Field> toRemove = new ArrayList<>();
        for (Field field : nonStaticValidFields) {
          if (fieldHandles.get(field).invoke(object) == null) {
            toRemove.add(field);
          }
        }
        nonStaticValidFields.removeAll(toRemove);
      }
      for (Field field : nonStaticValidFields) {
        VarHandle typeInfoHandle = lookup.unreflectVarHandle(field);
        Class<?> fieldType = typeInfoHandle.varType();
        String fieldName = lookup.revealDirect(fieldHandles.get(field))
            .getName()
            .replaceAll(followedByCapitalized, replacement)
            .replaceAll(followedByDigit, replacement)
            .toLowerCase();
        if (String.class.isAssignableFrom(fieldType) || Domain.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredString(fieldName);
        } else if (long.class.isAssignableFrom(fieldType)
            || BaseTraceObject.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredLong(fieldName);
        } else if (int.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredInt(fieldName);
        } else if (double.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder.requiredDouble(fieldName);
        } else if (long[].class.isAssignableFrom(fieldType)
            || Long[].class.isAssignableFrom(fieldType)
            || BaseTraceObject[].class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder
              .name(fieldName)
              .type()
              .array()
              .items()
              .longType()
              .noDefault();
        } else if (Map.class.isAssignableFrom(fieldType)) {
          schemaBuilder = schemaBuilder
              .name(fieldName)
              .type()
              .map()
              .values()
              .stringType()
              .noDefault();
        } else {
          log.error("Could not create a valid encoding for {}.", fieldType);
          throw new IllegalAccessException(fieldType.toString());
        }
        fieldsToSchema.put(lookup.revealDirect(fieldHandles.get(field)).getName(), fieldName);
      }
      avroSchema = schemaBuilder.endRecord();
    } catch (Throwable e) {
      log.error("Could not create a valid schema for {} in {}.", e.getMessage(), clazz);
      throw new RuntimeException("Could not create a valid schema for " + clazz, e);
    }
  }

  /**
   * Convert POJO to a {@link GenericRecord} to write it with the {@link org.apache.parquet.avro.AvroParquetWriter}.
   *
   * @param pojo      POJO to convert to a {@link GenericRecord}
   * @param clazz     {@link Class} to which the POJO belongs
   * @param <T>       type parameter for the {@link Class} and POJO
   * @return          {@link GenericRecord} containing the POJO
   */
  public <T> GenericRecord convertFromPojo(T pojo, Class<T> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    GenericData.Record record = new GenericData.Record(avroSchema);
    try {
      for (Field field : fields) {
        MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
        MethodHandle handle = lookup.unreflectGetter(field);
        if (!Modifier.isStatic(field.getModifiers())
            && fieldsToSchema.containsKey(
                lookup.revealDirect(handle).getName())) {
          Object object = handle.invoke(pojo);
          if (object instanceof BaseTraceObject[]) {
            object = Arrays.stream((BaseTraceObject[]) object)
                .map(BaseTraceObject::getId)
                .toArray();
          } else if (object instanceof Domain) {
            Domain domain = (Domain) object;
            switch (domain) {
              case INDUSTRIAL:
                object = "Industrial";
                break;
              case ENGINEERING:
                object = "Engineering";
                break;
              case SCIENTIFIC:
                object = "Scientific";
                break;
              case BIOMEDICAL:
                object = "Biomedical";
                break;
              default:
                object = "";
                log.error(
                    "Failed to properly serialise {} for value {}.",
                    lookup.revealDirect(handle).getName(),
                    domain);
                break;
            }
          } else if (object instanceof BaseTraceObject) {
            BaseTraceObject traceObject = (BaseTraceObject) object;
            object = traceObject.getId();
          }
          record.put(fieldsToSchema.get(lookup.revealDirect(handle).getName()), object);
        }
      }
    } catch (Throwable e) {
      log.error("Could not convert to Avro record {}.", e.getMessage());
    }
    log.debug("Converted record {}.", record);
    return record;
  }
}
