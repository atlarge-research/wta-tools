package com.asml.apa.wta.core.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Task class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Task implements BaseTraceObject {

  private static final long serialVersionUID = -1372345471722101373L;

  @Getter(value = AccessLevel.NONE)
  private final String schemaVersion = this.getSchemaVersion();

  private final long id;

  private final String type;

  private final long submitTime;

  private final int submissionSite;

  private final long runtime;

  private final String resourceType;

  private final double resourceAmountRequested;

  @Setter
  private long[] parents;

  @Setter
  private long[] children;

  private final int userId;

  private final int groupId;

  private final String nfrs;

  private final long workflowId;

  private final long waitTime;

  private final String params;

  private final double memoryRequested;

  private final long networkIoTime;

  private final long diskIoTime;

  private final double diskSpaceRequested;

  private final double energyConsumption;

  private final long resourceUsed;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   * It will put all fields allowed by the checker into the record.
   *
   * @param checker checker for which column to skip
   * @param schema schema
   * @return record
   * @since 1.0.0
   * @author Tianchen Qu
   */
  @SuppressWarnings("CyclomaticComplexity")
  public GenericRecord convertToRecord(Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0]) {
      record.put("id", this.id);
    }
    if (checker[1]) {
      record.put("type", this.type);
    }
    if (checker[2]) {
      record.put("ts_submit", this.submitTime);
    }
    if (checker[3]) {
      record.put("submission_site", this.submissionSite);
    }
    if (checker[4]) {
      record.put("runtime", this.runtime);
    }
    if (checker[5]) {
      record.put("resource_type", this.resourceType);
    }
    if (checker[6]) {
      record.put("resource_amount_requested", this.resourceAmountRequested);
    }
    if (checker[7]) {
      record.put("parents", this.parents);
    }
    if (checker[8]) {
      record.put("children", this.children);
    }
    if (checker[9]) {
      record.put("user_id", this.userId);
    }
    if (checker[10]) {
      record.put("group_id", this.groupId);
    }
    if (checker[11]) {
      record.put("nfrs", this.nfrs);
    }
    if (checker[12]) {
      record.put("workflow_id", this.workflowId);
    }
    if (checker[13]) {
      record.put("wait_time", this.waitTime);
    }
    if (checker[14]) {
      record.put("params", this.params);
    }
    if (checker[15]) {
      record.put("memory_requested", this.memoryRequested);
    }
    if (checker[16]) {
      record.put("network_io_time", this.networkIoTime);
    }
    if (checker[17]) {
      record.put("disk_io_time", this.diskIoTime);
    }
    if (checker[18]) {
      record.put("disk_space_requested", this.diskSpaceRequested);
    }
    if (checker[19]) {
      record.put("energy_consumption", this.energyConsumption);
    }
    if (checker[20]) {
      record.put("resource_used", this.resourceUsed);
    }
    return record;
  }
}
