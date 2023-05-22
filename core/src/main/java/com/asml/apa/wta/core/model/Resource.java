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
   * convert resource object to record.
   *
   * @param resource resource
   * @return record
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public static GenericRecord convertResourceToRecord(Resource resource, Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0] == true) {
      record.put("id", resource.getId());
    }
    if (checker[1] == true) {
      record.put("type", resource.getType());
    }
    if (checker[2] == true) {
      record.put("numResources", resource.getNumResources());
    }
    if (checker[3] == true) {
      record.put("procModel", resource.getProcModel());
    }
    if (checker[4] == true) {
      record.put("memory", resource.getMemory());
    }
    if (checker[5] == true) {
      record.put("diskSpace", resource.getDiskSpace());
    }
    if (checker[6] == true) {
      record.put("networkSpeed", resource.getNetworkSpeed());
    }
    if (checker[7] == true) {
      record.put("os", resource.getOs());
    }
    if (checker[8] == true) {
      record.put("details", resource.getDetails());
    }
    return record;
  }
}
