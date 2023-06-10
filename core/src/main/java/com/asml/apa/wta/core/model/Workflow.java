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

  private final long criticalPathLength;

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

  private final double totalEnergyConsumption;

  /**
   * Converts the POJO object into record object, enabling it to be written by Avro.
   * It will put all fields allowed by the checker into the record.
   *
   * @param checker checker for which column to skip
   * @param schema schema The Avro schema
   * @return record The record that corresponds to a row in parquet
   * @author Tianchen Qu
   * @since 1.0.0
   */
  @Override
  @SuppressWarnings("CyclomaticComplexity")
  public GenericRecord convertToRecord(Boolean[] checker, Schema schema) {
    GenericData.Record record = new GenericData.Record(schema);
    if (checker[0]) {
      record.put("id", this.id);
    }
    if (checker[1]) {
      record.put("ts_submit", this.submitTime);
    }
    if (checker[2]) {
      record.put("tasks", Arrays.stream(this.tasks).map(Task::getId).toArray());
    }
    if (checker[3]) {
      record.put("task_count", this.numberOfTasks);
    }
    if (checker[4]) {
      record.put("critical_path_length", this.criticalPathLength);
    }
    if (checker[5]) {
      record.put("critical_path_task_count", this.criticalPathTaskCount);
    }
    if (checker[6]) {
      record.put("max_concurrent_tasks", this.maxNumberOfConcurrentTasks);
    }
    if (checker[7]) {
      record.put("nfrs", this.nfrs);
    }
    if (checker[8]) {
      record.put("scheduler", this.scheduler);
    }
    if (checker[9]) {
      record.put("domain", this.domain);
    }
    if (checker[10]) {
      record.put("application_name", this.applicationName);
    }
    if (checker[11]) {
      record.put("application_field", this.applicationField);
    }
    if (checker[12]) {
      record.put("total_resources", this.totalResources);
    }
    if (checker[13]) {
      record.put("total_memory_usage", this.totalMemoryUsage);
    }
    if (checker[14]) {
      record.put("total_network_usage", this.totalNetworkUsage);
    }
    if (checker[15]) {
      record.put("total_disk_space_usage", this.totalDiskSpaceUsage);
    }
    if (checker[16]) {
      record.put("total_energy_consumption", this.totalEnergyConsumption);
    }
    return record;
  }
}
