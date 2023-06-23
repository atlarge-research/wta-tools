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

  private final Resource resourceId;

  private final long timestamp;

  private final String eventType;

  private final long platformId;

  private final double availableResources;

  private final double availableMemory;

  private final double availableDiskSpace;

  private final double availableDiskIoBandwidth;

  private final double availableNetworkBandwidth;

  private final double averageUtilization1Minute;

  private final double averageUtilization5Minute;

  private final double averageUtilization15Minute;

  /**
   * This method should never be called as we do not need to ever fetch its ID.
   *
   * @throws RuntimeException always
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
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
