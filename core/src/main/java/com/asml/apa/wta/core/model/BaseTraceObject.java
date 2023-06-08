package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;

/**
 * BaseSchema interface that all schema objects implement.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
public interface BaseTraceObject extends Serializable {

  /**
   * Returns a hardcoded schema version.
   *
   * @return The associated config object
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  default String getSchemaVersion() {
    return "1.0";
  }

  /**
   * All WTA objects that are stored as Parquet files rely on this method to convert the object to a record.
   * It should build the record object based on the checker and the schema provided.
   *
   * @param schema schema for the output object
   * @return record of the object
   */
  GenericRecord convertToRecord(ParquetSchema schema);
}
