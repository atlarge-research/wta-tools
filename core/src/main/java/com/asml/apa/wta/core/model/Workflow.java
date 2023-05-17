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

  private static Schema workflowSchema = SchemaBuilder.record("workflow")
      .namespace("com.asml.apa.wta.core.model")
      .fields()
      .name("id")
      .type()
      .longType()
      .noDefault()
      .name("submitTime")
      .type()
      .longType()
      .noDefault()
      .name("tasks")
      .type()
      .nullable()
      .array()
      .items()
      .longType()
      .noDefault()
      .name("numberOfTasks")
      .type()
      .intType()
      .noDefault()
      .name("criticalPathLength")
      .type()
      .intType()
      .noDefault()
      .name("criticalPathTaskCount")
      .type()
      .intType()
      .noDefault()
      .name("maxNumberOfConcurrentTasks")
      .type()
      .intType()
      .noDefault()
      .name("nfrs")
      .type()
      .nullable()
      .stringType()
      .noDefault()
      .name("scheduler")
      .type()
      .nullable()
      .stringType()
      .noDefault()
      .name("domain")
      .type()
      .nullable()
      .stringType()
      .noDefault()
      .name("applicationName")
      .type()
      .nullable()
      .stringType()
      .noDefault()
      .name("applicationField")
      .type()
      .nullable()
      .stringType()
      .noDefault()
      .name("totalResources")
      .type()
      .doubleType()
      .noDefault()
      .name("totalMemoryUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("totalNetworkUsage")
      .type()
      .longType()
      .noDefault()
      .name("totalDiskSpaceUsage")
      .type()
      .longType()
      .noDefault()
      .name("totalEnergyConsumption")
      .type()
      .longType()
      .noDefault()
      .endRecord();

  public static GenericRecord convertWorkflowToRecord(Workflow workflow) {
    GenericData.Record record = new GenericData.Record(workflowSchema);
    record.put("id", workflow.id);
    record.put("applicationField", workflow.applicationField);
    record.put("applicationName", workflow.applicationName);
    String domain;
    if (workflow.domain == null) {
      domain = null;
    } else {
      domain = workflow.domain.getValue();
    }
    record.put("domain", domain);
    record.put("nfrs", workflow.nfrs);
    record.put("criticalPathLength", workflow.criticalPathLength);
    record.put("criticalPathTaskCount", workflow.criticalPathTaskCount);
    record.put("maxNumberOfConcurrentTasks", workflow.maxNumberOfConcurrentTasks);
    record.put("numberOfTasks", workflow.numberOfTasks);
    record.put("scheduler", workflow.scheduler);
    record.put("submitTime", workflow.submitTime);
    Long[] tasks;
    if (workflow.tasks == null) {
      tasks = null;
    } else {
      tasks = (Long[]) Arrays.stream(workflow.tasks).map(x -> x.getId()).toArray();
    }
    record.put("tasks", tasks);
    record.put("totalDiskSpaceUsage", workflow.totalDiskSpaceUsage);
    record.put("totalEnergyConsumption", workflow.totalEnergyConsumption);
    record.put("totalMemoryUsage", workflow.totalMemoryUsage);
    record.put("totalNetworkUsage", workflow.totalNetworkUsage);
    record.put("totalResources", workflow.totalResources);
    return record;
  }

  public static Schema getWorkflowSchema() {
    return workflowSchema;
  }
}
