package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;

/**
 * Resource class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
@SuppressWarnings("VisibilityModifier")
public class Resource implements BaseTraceObject {

  private static final long serialVersionUID = 3002249398331752973L;

  public final long id;

  @Builder.Default
  public final String type = "cluster node";

  public final double numResources;

  public final String procModel;

  public final long memory;

  public final long diskSpace;

  public final long network;

  public final String os;

  @Builder.Default
  public final String details = "";

  public final Map<String, String> events;

  /**
   * All WTA objects that are stored as Parquet files rely on this method to convert the object to a record.
   * It should build the record object based on the checker and the schema provided.
   *
   * @param schema schema for the output object
   * @return record of the object
   */
  @Override
  public GenericRecord convertToRecord(ParquetSchema schema) {
    return schema.convertFromPojo(this, Resource.class);
  }
}
