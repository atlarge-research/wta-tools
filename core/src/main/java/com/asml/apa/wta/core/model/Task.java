package com.asml.apa.wta.core.model;

import java.util.ArrayList;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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

  private static Schema taskSchema = SchemaBuilder.record("task")
      .namespace("com.asml.apa.wta.core.model")
      .fields()
      .name("id")
      .type()
      .longType()
      .noDefault()
      .name("type")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .name("submitType")
      .type()
      .longType()
      .longDefault(0)
      .name("submissionSite")
      .type()
      .intType()
      .intDefault(0)
      .name("runtime")
      .type()
      .longType()
      .longDefault(0)
      .name("resourceType")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .name("resourceAmountRequested")
      .type()
      .doubleType()
      .doubleDefault(0.0)
      .name("parents")
      .type()
      .nullable()
      .array()
      .items()
      .longType()
      .arrayDefault(new ArrayList<Long>())
      .name("children")
      .type()
      .nullable()
      .array()
      .items()
      .longType()
      .arrayDefault(new ArrayList<Long>())
      .name("userId")
      .type()
      .intType()
      .intDefault(0)
      .name("groupId")
      .type()
      .intType()
      .intDefault(0)
      .name("nfrs")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .name("workflowId")
      .type()
      .longType()
      .longDefault(0)
      .name("waitTime")
      .type()
      .longType()
      .longDefault(0)
      .name("params")
      .type()
      .nullable()
      .stringType()
      .stringDefault("test")
      .name("memoryRequested")
      .type()
      .doubleType()
      .doubleDefault(0.0)
      .name("networkIoTime")
      .type()
      .longType()
      .longDefault(0)
      .name("diskIoTime")
      .type()
      .longType()
      .longDefault(0)
      .name("diskSpaceRequested")
      .type()
      .doubleType()
      .doubleDefault(0.0)
      .name("energyConsumption")
      .type()
      .longType()
      .longDefault(0)
      .name("resourceUsed")
      .type()
      .longType()
      .longDefault(0)
      .endRecord();

  /**
   * convert task to record.
   *
   * @param task task
   * @return record
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public static GenericRecord convertTaskToRecord(Task task) {
    GenericData.Record record = new GenericData.Record(taskSchema);
    record.put("id", task.id);
    record.put("type", task.type);
    record.put("submitType", task.submitType);
    record.put("submissionSite", task.submissionSite);
    record.put("runtime", task.runtime);
    record.put("resourceType", task.resourceType);
    record.put("resourceAmountRequested", task.resourceAmountRequested);
    record.put("parents", task.parents);
    record.put("children", task.children);
    record.put("userId", task.userId);
    record.put("groupId", task.groupId);
    record.put("nfrs", task.nfrs);
    record.put("workflowId", task.workflowId);
    record.put("waitTime", task.waitTime);
    record.put("params", task.params);
    record.put("memoryRequested", task.memoryRequested);
    record.put("networkIoTime", task.networkIoTime);
    record.put("diskIoTime", task.diskIoTime);
    record.put("diskSpaceRequested", task.diskSpaceRequested);
    record.put("energyConsumption", task.energyConsumption);
    record.put("resourceUsed", task.resourceUsed);
    return record;
  }

  public static Schema getTaskSchema() {
    return taskSchema;
  }
}
