package com.asml.apa.wta.core.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Resource class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Resource implements BaseTraceObject {

  private static final long serialVersionUID = 3002249398331752973L;

  @Getter(value = AccessLevel.NONE)
  private final String schemaVersion = this.getSchemaVersion();

  private final long id;

  private final String type;

  private final double numResources;

  private final String procModel;

  private final long memory;

  private final long diskSpace;

  private final long networkSpeed;

  private final String os;

  private final String details;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   * It will put all fields allowed by the checker into the record.
   *
   * @param checker checker for which column to skip
   * @param schema schema The Avro schema
   * @return record The record that corresponds to a row in parquet
   * @since 1.0.0
   * @author Tianchen Qu
   */
  @Override
  @SuppressWarnings("CyclomaticComplexity")
  public GenericRecord convertToRecord(Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0]) {
      record.put("id", this.getId());
    }
    if (checker[1]) {
      record.put("type", this.getType());
    }
    if (checker[2]) {
      record.put("num_resources", this.getNumResources());
    }
    if (checker[3]) {
      record.put("proc_model", this.getProcModel());
    }
    if (checker[4]) {
      record.put("memory", this.getMemory());
    }
    if (checker[5]) {
      record.put("disk_space", this.getDiskSpace());
    }
    if (checker[6]) {
      record.put("network", this.getNetworkSpeed());
    }
    if (checker[7]) {
      record.put("os", this.getOs());
    }
    if (checker[8]) {
      record.put("details", this.getDetails());
    }
    return record;
  }
}
