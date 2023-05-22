package com.asml.apa.wta.core.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
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

  private final long submitType;

  private final int submissionSite;

  private final long runtime;

  private final String resourceType;

  private final double resourceAmountRequested;

  private final long[] parents;

  private final long[] children;

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

  private final long energyConsumption;

  private final long resourceUsed;

  /**
   * convert task to record.
   *
   * @param task task
   * @return record
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public static GenericRecord convertTaskToRecord(Task task, Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0] == true) {
      record.put("id", task.id);
    }
    if (checker[1] == true) {
      record.put("type", task.type);
    }
    if (checker[2] == true) {
      record.put("submitType", task.submitType);
    }
    if (checker[3] == true) {
      record.put("submissionSite", task.submissionSite);
    }
    if (checker[4] == true) {
      record.put("runtime", task.runtime);
    }
    if (checker[5] == true) {
      record.put("resourceType", task.resourceType);
    }
    if (checker[6] == true) {
      record.put("resourceAmountRequested", task.resourceAmountRequested);
    }
    if (checker[7] == true) {
      record.put("parents", task.parents);
    }
    if (checker[8] == true) {
      record.put("children", task.children);
    }
    if (checker[9] == true) {
      record.put("userId", task.userId);
    }
    if (checker[10] == true) {
      record.put("groupId", task.groupId);
    }
    if (checker[11] == true) {
      record.put("nfrs", task.nfrs);
    }
    if (checker[12] == true) {
      record.put("workflowId", task.workflowId);
    }
    if (checker[13] == true) {
      record.put("waitTime", task.waitTime);
    }
    if (checker[14] == true) {
      record.put("params", task.params);
    }
    if (checker[15] == true) {
      record.put("memoryRequested", task.memoryRequested);
    }
    if (checker[16] == true) {
      record.put("networkIoTime", task.networkIoTime);
    }
    if (checker[17] == true) {
      record.put("diskIoTime", task.diskIoTime);
    }
    if (checker[18] == true) {
      record.put("diskSpaceRequested", task.diskSpaceRequested);
    }
    if (checker[19] == true) {
      record.put("energyConsumption", task.energyConsumption);
    }
    if (checker[20] == true) {
      record.put("resourceUsed", task.resourceUsed);
    }
    return record;
  }
}
