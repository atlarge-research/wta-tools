package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.avro.Schema;
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

  private final double totalDiskSpaceUsage;

  private final long totalEnergyConsumption;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   *
   * @param workflow workflow
   * @param checker checker for which column to skip
   * @param schema schema
   * @return record
   * @since 1.0.0
   * @author Tianchen Qu
   */
  @SuppressWarnings("CyclomaticComplexity")
  public static GenericRecord convertWorkflowToRecord(Workflow workflow, Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0]) {
      record.put("id", workflow.id);
    }
    if (checker[1]) {
      record.put("ts_submit", workflow.submitTime);
    }
    if (checker[2]) {
      record.put("tasks", Arrays.stream(workflow.tasks).map(Task::getId).toArray());
    }
    if (checker[3]) {
      record.put("task_count", workflow.numberOfTasks);
    }
    if (checker[4]) {
      record.put("critical_path_length", workflow.criticalPathLength);
    }
    if (checker[5]) {
      record.put("critical_path_task_count", workflow.criticalPathTaskCount);
    }
    if (checker[6]) {
      record.put("max_concurrent_tasks", workflow.maxNumberOfConcurrentTasks);
    }
    if (checker[7]) {
      record.put("nfrs", workflow.nfrs);
    }
    if (checker[8]) {
      record.put("scheduler", workflow.scheduler);
    }
    if (checker[9]) {
      record.put("domain", workflow.domain);
    }
    if (checker[10]) {
      record.put("application_name", workflow.applicationName);
    }
    if (checker[11]) {
      record.put("application_field", workflow.applicationField);
    }
    if (checker[12]) {
      record.put("total_resources", workflow.totalResources);
    }
    if (checker[13]) {
      record.put("total_memory_usage", workflow.totalMemoryUsage);
    }
    if (checker[14]) {
      record.put("total_network_usage", workflow.totalNetworkUsage);
    }
    if (checker[15]) {
      record.put("total_disk_space_usage", workflow.totalDiskSpaceUsage);
    }
    if (checker[16]) {
      record.put("total_energy_consumption", workflow.totalEnergyConsumption);
    }
    return record;
  }
}
