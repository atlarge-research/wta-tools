package com.asml.apa.wta.core.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * ResourceState class corresponding to WTA format.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Data
@Builder
public class ResourceState implements BaseTraceObject {

  private static final long serialVersionUID = 3002249398331752973L;

  @Getter(value = AccessLevel.NONE)
  private final String schemaVersion = this.getSchemaVersion();

  private long resourceId;

  private long timestamp;

  private String eventType;

  private long platformId;

  private double availableResources;

  private double availableMemory;

  private double availableDiskIoBandwidth;

  private double availableNetworkBandwidth;

  private double averageUtilization1Minute;
  private double averageUtilization5Minute;
  private double averageUtilization15Minute;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   * It will put all fields allowed by the checker into the record.
   *
   * @param checker checker for which column to skip
   * @param schema schema The Avro schema
   * @return record A record that corresponds to a row in parquet
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  @SuppressWarnings("CyclomaticComplexity")
  public GenericRecord convertToRecord(Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0]) {
      record.put("resource_id", this.resourceId);
    }
    if (checker[1]) {
      record.put("timestamp", this.timestamp);
    }
    if (checker[2]) {
      record.put("event_type", this.eventType);
    }
    if (checker[3]) {
      record.put("platform_id", this.platformId);
    }
    if (checker[4]) {
      record.put("available_resources", this.availableResources);
    }
    if (checker[5]) {
      record.put("available_memory", this.availableMemory);
    }
    if (checker[6]) {
      record.put("available_disk_io_bandwidth", this.availableDiskIoBandwidth);
    }
    if (checker[7]) {
      record.put("available_network_bandwidth", this.availableNetworkBandwidth);
    }
    if (checker[8]) {
      record.put("average_utilization_1_minute", this.averageUtilization1Minute);
    }
    if (checker[9]) {
      record.put("average_utilization_5_minute", this.averageUtilization5Minute);
    }
    if (checker[10]) {
      record.put("average_utilization_15_minute", this.averageUtilization15Minute);
    }
    return record;
  }
}
