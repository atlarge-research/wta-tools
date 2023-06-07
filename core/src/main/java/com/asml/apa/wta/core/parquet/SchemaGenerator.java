package com.asml.apa.wta.core.parquet;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Schema generator for writing to Parquet files.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class SchemaGenerator {

  /**
   * Constructs a schema that would be densely filled by the {@link Collection} of objects.
   *
   * @param clazz the {@link Class} to construct the schema for
   * @param objects the {@link Collection} of objects to construct the schema for
   * @return a {@link MessageType} schema for the objects
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public <T> MessageType constructSchema(@NonNull Class<T> clazz, String name, Collection<T> objects) {
    String regex = "([a-z])([A-Z]+)";
    String replacement = "$1_$2";
    Field[] fields = clazz.getDeclaredFields();
    List<Type> types = new ArrayList<>();
    try {
      for (Field field : fields) {
        boolean sparseField = false;
        for (T o : objects) {
          if (field.get(o) == null) {
            sparseField = true;
            break;
          }
        }
        if (!sparseField) {
          Types.PrimitiveBuilder<PrimitiveType> typeBuilder;
          Class<?> fieldType = field.getType();
          if (String.class.isAssignableFrom(fieldType)) {
            typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                .as(LogicalTypeAnnotation.stringType());
          } else if (Long.class.isAssignableFrom(fieldType)) {
            typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED);
          } else if (Integer.class.isAssignableFrom(fieldType)) {
            typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED);
          } else if (Double.class.isAssignableFrom(fieldType)) {
            typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED);
          } else if (Float.class.isAssignableFrom(fieldType)) {
            typeBuilder = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.REQUIRED);
          } else if (Boolean.class.isAssignableFrom(fieldType)) {
            typeBuilder =
                Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED);
          } else {
            throw new IllegalAccessException();
          }
          types.add(typeBuilder.named(
              field.getName().replaceAll(regex, replacement).toLowerCase()));
        }
      }
      return new MessageType(name, types);
    } catch (IllegalAccessException e) {
      log.error("Could not create a valid schema for {}", clazz);
      throw new RuntimeException("Could not create a valid schema for " + clazz);
    }
  }
}
