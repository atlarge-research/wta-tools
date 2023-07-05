package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

/**
 * ResourceState class corresponding to WTA format.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Data
@Slf4j
@Builder
public class ResourceState implements BaseTraceObject {

  private static final long serialVersionUID = 8912154769719138654L;

  private final long resourceId;

  @Builder.Default
  private final long timestamp = -1L;

  @Builder.Default
  private final String eventType = "";

  @Builder.Default
  private final long platformId = -1L;

  @Builder.Default
  private final double availableResources = -1.0;

  @Builder.Default
  private final double availableMemory = -1.0;

  @Builder.Default
  private final double availableDiskSpace = -1.0;

  @Builder.Default
  private final double availableDiskIoBandwidth = -1.0;

  @Builder.Default
  private final double availableNetworkBandwidth = -1.0;

  @Builder.Default
  private final double averageUtilization1Minute = -1.0;

  @Builder.Default
  private final double averageUtilization5Minute = -1.0;

  @Builder.Default
  private final double averageUtilization15Minute = -1.0;

  /**
   * This method should never be called as we do not need to ever fetch its ID.
   *
   * @throws RuntimeException always
   */
  @Override
  public final long getId() {
    log.error(
        "The application attempted to get the id of a ResourceState, this is illegal and should never happen.");
    throw accessError();
  }

  /**
   * All WTA objects that are stored as Parquet files rely on this method to convert the object to a record.
   * It should build the record object based on the checker and the schema provided.
   *
   * @param schema schema for the output object
   * @return record of the object
   */
  @Override
  public GenericRecord convertToRecord(ParquetSchema schema) {
    log.trace("Converting ResourceState with Resource id to record");
    return schema.convertFromPojo(this, ResourceState.class);
  }
}
