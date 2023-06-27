package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

/**
 * Resource class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@Slf4j
public class Resource implements BaseTraceObject {

  private static final long serialVersionUID = 3002249398331752973L;

  private final long id;

  @Builder.Default
  private final String type = "cluster node";

  @Builder.Default
  private final double numResources = 1.0;

  @Builder.Default
  private final String procModel = "unknown";

  @Builder.Default
  private final long memory = -1L;

  @Builder.Default
  private final long diskSpace = -1L;

  @Builder.Default
  private final long network = -1L;

  @Builder.Default
  private final String os = "unknown";

  @Builder.Default
  private final String details = "";

  @Builder.Default
  private final Map<String, String> events = new HashMap<>();

  /**
   * All WTA objects that are stored as Parquet files rely on this method to convert the object to a record.
   * It should build the record object based on the checker and the schema provided.
   *
   * @param schema schema for the output object
   * @return record of the object
   */
  @Override
  public GenericRecord convertToRecord(ParquetSchema schema) {
    log.trace("Converting Resource with id {} to record", this.id);
    return schema.convertFromPojo(this, Resource.class);
  }
}
