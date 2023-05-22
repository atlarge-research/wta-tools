package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Workflow class corresponding to WTA format.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Workflow implements BaseTraceObject {

  private static final long serialVersionUID = 9065743819019553490L;

  @Getter(value = AccessLevel.NONE)
  private final String schemaVersion = this.getSchemaVersion();

  private final long id;

  private final long submitTime;

  private final Task[] tasks;

  private final int numberOfTasks;

  private final int criticalPathLength;

  private final int criticalPathTaskCount;

  private final int maxNumberOfConcurrentTasks;

  private final String nfrs;

  private final String scheduler;

  private final Domain domain;

  private final String applicationName;

  private final String applicationField;

  private final double totalResources;

  private final double totalMemoryUsage;

  private final long totalNetworkUsage;

  private final long totalDiskSpaceUsage;

  private final long totalEnergyConsumption;


  /**
   * convert workflow to record.
   *
   * @param workflow workflow
   * @return record
   * @since 1.0.0
   * @author Tianchen Qu
   */
  public static GenericRecord convertWorkflowToRecord(Workflow workflow, Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0] == true) {
      record.put("id", workflow.id);
    }
    if (checker[1] == true) {
      record.put("submitTime", workflow.submitTime);
    }
    if (checker[2] == true) {
      record.put("tasks", (Long[])
          Arrays.stream(workflow.tasks).map(x -> x.getId()).toArray());
    }
    if (checker[3] == true) {
      record.put("numberOfTasks", workflow.numberOfTasks);
    }
    if (checker[4] == true) {
      record.put("criticalPathLength", workflow.criticalPathLength);
    }
    if (checker[5] == true) {
      record.put("criticalPathTaskCount", workflow.criticalPathTaskCount);
    }
    if (checker[6] == true) {
      record.put("maxNumberOfConcurrentTasks", workflow.maxNumberOfConcurrentTasks);
    }
    if (checker[7] == true) {
      record.put("nfrs", workflow.nfrs);
    }
    if (checker[8] == true) {
      record.put("scheduler", workflow.scheduler);
    }
    if (checker[9] == true) {
      record.put("domain", workflow.domain.getValue());
    }
    if (checker[10] == true) {
      record.put("applicationName", workflow.applicationName);
    }
    if (checker[11] == true) {
      record.put("applicationField", workflow.applicationField);
    }
    if (checker[12] == true) {
      record.put("totalResources", workflow.totalResources);
    }
    if (checker[13] == true) {
      record.put("totalMemoryUsage", workflow.totalMemoryUsage);
    }
    if (checker[14] == true) {
      record.put("totalNetworkUsage", workflow.totalNetworkUsage);
    }
    if (checker[15] == true) {
      record.put("totalDiskSpaceUsage", workflow.totalDiskSpaceUsage);
    }
    if (checker[16] == true) {
      record.put("totalEnergyConsumption", workflow.totalEnergyConsumption);
    }
    return record;
  }
}
