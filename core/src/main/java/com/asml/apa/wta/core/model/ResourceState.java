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
@Builder
@Slf4j
@SuppressWarnings("VisibilityModifier")
public class ResourceState implements BaseTraceObject {

  private static final long serialVersionUID = 8912154769719138654L;

  public final Resource resourceId;

  public final long timestamp;

  public final String eventType;

  public final long platformId;

  public final double availableResources;

  public final double availableMemory;

  public final double availableDiskSpace;

  public final double availableDiskIoBandwidth;

  public final double availableNetworkBandwidth;

  public final double averageUtilization1Minute;

  public final double averageUtilization5Minute;

  public final double averageUtilization15Minute;

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
    return schema.convertFromPojo(this, ResourceState.class);
  }
}
