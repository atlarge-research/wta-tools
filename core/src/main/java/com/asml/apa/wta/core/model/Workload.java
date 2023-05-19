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
 * Workload class corresponding to WTA format.
 *
 * @author  Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@Builder
public class Workload implements BaseTraceObject {
  private static final long serialVersionUID = -4547341610378381743L;

  @Getter(value = AccessLevel.NONE)
  private final String schemaVersion = this.getSchemaVersion();

  private final Workflow[] workflows;

  private final long totalWorkflows;

  private final long totalTasks;

  private final Domain domain;

  private final long startDate;

  private final long endDate;

  private final long numSites;

  private final long numResources;

  private final long numUsers;

  private final long numGroups;

  private final double totalResourceSeconds;

  private final String[] authors;

  private final double minResourceTask;

  private final double maxResourceTask;

  private final double stdResourceTask;

  private final double meanResourceTask;

  private final double medianResourceTask;

  private final double firstQuartileResourceTask;

  private final double thirdQuartileResourceTask;

  private final double covResourceTask;

  private final double minMemory;

  private final double maxMemory;

  private final double stdMemory;

  private final double meanMemory;

  private final double medianMemory;

  private final long firstQuartileMemory;

  private final long thirdQuartileMemory;

  private final double covMemory;

  private final long minNetworkUsage;

  private final long maxNetworkUsage;

  private final double stdNetworkUsage;

  private final double meanNetworkUsage;

  private final double medianNetworkUsage;

  private final long firstQuartileNetworkUsage;

  private final long thirdQuartileNetworkUsage;

  private final double covNetworkUsage;

  private final double minDiskSpaceUsage;

  private final double maxDiskSpaceUsage;

  private final double stdDiskSpaceUsage;

  private final double meanDiskSpaceUsage;

  private final long medianDiskSpaceUsage;

  private final long firstQuartileDiskSpaceUsage;

  private final long thirdQuartileDiskSpaceUsage;

  private final double covDiskSpaceUsage;

  private final int minEnergy;

  private final int maxEnergy;

  private final double stdEnergy;

  private final double meanEnergy;

  private final int medianEnergy;

  private final int firstQuartileEnergy;

  private final int thirdQuartileEnergy;

  private final double covEnergy;

  private final String workloadDescription;

  private static Schema workloadSchema = SchemaBuilder.record("workload")
      .namespace("com.asml.apa.wta.core.model")
      .fields()
      .name("workflows")
      .type()
      .nullable()
      .array()
      .items()
      .longType()
      .noDefault()
      .name("totalWorkflows")
      .type()
      .longType()
      .noDefault()
      .name("totalTasks")
      .type()
      .longType()
      .noDefault()
      .name("domain")
      .type()
      .nullable()
      .stringType()
      .noDefault()
      .name("startDate")
      .type()
      .longType()
      .noDefault()
      .name("endDate")
      .type()
      .longType()
      .noDefault()
      .name("numSites")
      .type()
      .longType()
      .noDefault()
      .name("numResources")
      .type()
      .longType()
      .noDefault()
      .name("numUsers")
      .type()
      .longType()
      .noDefault()
      .name("numGroups")
      .type()
      .longType()
      .noDefault()
      .name("totalResourceSeconds")
      .type()
      .doubleType()
      .noDefault()
      .name("authors")
      .type()
      .nullable()
      .array()
      .items()
      .nullable()
      .stringType()
      .noDefault()
      .name("minResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("maxResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("stdResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("meanResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("medianResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("firstQuartileResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("thirdQuartileResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("covResourceTask")
      .type()
      .doubleType()
      .noDefault()
      .name("minMemory")
      .type()
      .doubleType()
      .noDefault()
      .name("maxMemory")
      .type()
      .doubleType()
      .noDefault()
      .name("stdMemory")
      .type()
      .doubleType()
      .noDefault()
      .name("meanMemory")
      .type()
      .doubleType()
      .noDefault()
      .name("medianMemory")
      .type()
      .doubleType()
      .noDefault()
      .name("firstQuartileMemory")
      .type()
      .longType()
      .noDefault()
      .name("thirdQuartileMemory")
      .type()
      .longType()
      .noDefault()
      .name("covMemory")
      .type()
      .doubleType()
      .noDefault()
      .name("minNetworkUsage")
      .type()
      .longType()
      .noDefault()
      .name("maxNetworkUsage")
      .type()
      .longType()
      .noDefault()
      .name("stdNetworkUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("meanNetworkUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("medianNetworkUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("firstQuartileNetworkUsage")
      .type()
      .longType()
      .noDefault()
      .name("thirdQuartileNetworkUsage")
      .type()
      .longType()
      .noDefault()
      .name("covNetworkUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("minDiskSpaceUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("maxDiskSpaceUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("stdDiskSpaceUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("meanDiskSpaceUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("medianDiskSpaceUsage")
      .type()
      .longType()
      .noDefault()
      .name("firstQuartileDiskSpaceUsage")
      .type()
      .longType()
      .noDefault()
      .name("thirdQuartileDiskSpaceUsage")
      .type()
      .longType()
      .noDefault()
      .name("covDiskSpaceUsage")
      .type()
      .doubleType()
      .noDefault()
      .name("minEnergy")
      .type()
      .intType()
      .noDefault()
      .name("maxEnergy")
      .type()
      .intType()
      .noDefault()
      .name("stdEnergy")
      .type()
      .doubleType()
      .noDefault()
      .name("meanEnergy")
      .type()
      .doubleType()
      .noDefault()
      .name("medianEnergy")
      .type()
      .intType()
      .noDefault()
      .name("firstQuartileEnergy")
      .type()
      .intType()
      .noDefault()
      .name("thirdQuartileEnergy")
      .type()
      .intType()
      .noDefault()
      .name("covEnergy")
      .type()
      .doubleType()
      .noDefault()
      .name("workloadDescription")
      .type()
      .nullable()
      .stringType()
      .noDefault()
      .endRecord();

  /**convert workload to record.
   *
   * @param workload workload
   * @return record
   */
  public static GenericRecord convertWorkloadToRecord(Workload workload) {
    GenericData.Record record = new GenericData.Record(workloadSchema);
    record.put("authors", workload.authors);
    record.put("covDiskSpaceUsage", workload.covDiskSpaceUsage);
    record.put("covEnergy", workload.covEnergy);
    record.put("covMemory", workload.covMemory);
    record.put("covNetworkUsage", workload.covNetworkUsage);
    record.put("covResourceTask", workload.covResourceTask);
    String domain;
    if (workload.domain == null) {
      domain = null;
    } else {
      domain = workload.domain.getValue();
    }
    record.put("domain", domain);
    record.put("endDate", workload.endDate);
    record.put("firstQuartileDiskSpaceUsage", workload.firstQuartileDiskSpaceUsage);
    record.put("firstQuartileEnergy", workload.firstQuartileEnergy);
    record.put("firstQuartileMemory", workload.firstQuartileMemory);
    record.put("firstQuartileNetworkUsage", workload.firstQuartileNetworkUsage);
    record.put("firstQuartileResourceTask", workload.firstQuartileResourceTask);
    record.put("maxDiskSpaceUsage", workload.maxDiskSpaceUsage);
    record.put("maxEnergy", workload.maxEnergy);
    record.put("maxMemory", workload.maxMemory);
    record.put("maxResourceTask", workload.maxResourceTask);
    record.put("maxNetworkUsage", workload.maxNetworkUsage);
    record.put("meanDiskSpaceUsage", workload.meanDiskSpaceUsage);
    record.put("meanEnergy", workload.meanEnergy);
    record.put("meanMemory", workload.meanMemory);
    record.put("meanNetworkUsage", workload.meanNetworkUsage);
    record.put("meanResourceTask", workload.meanResourceTask);
    record.put("medianDiskSpaceUsage", workload.medianDiskSpaceUsage);
    record.put("medianEnergy", workload.medianEnergy);
    record.put("medianMemory", workload.medianMemory);
    record.put("medianNetworkUsage", workload.medianNetworkUsage);
    record.put("medianResourceTask", workload.medianResourceTask);
    record.put("minDiskSpaceUsage", workload.minDiskSpaceUsage);
    record.put("minEnergy", workload.minEnergy);
    record.put("minMemory", workload.minMemory);
    record.put("minNetworkUsage", workload.minNetworkUsage);
    record.put("minResourceTask", workload.minResourceTask);
    record.put("numGroups", workload.numGroups);
    record.put("numResources", workload.numResources);
    record.put("numSites", workload.numSites);
    record.put("numUsers", workload.numUsers);
    record.put("startDate", workload.startDate);
    record.put("stdEnergy", workload.stdEnergy);
    record.put("stdDiskSpaceUsage", workload.stdDiskSpaceUsage);
    record.put("stdMemory", workload.stdMemory);
    record.put("stdNetworkUsage", workload.stdNetworkUsage);
    record.put("stdResourceTask", workload.stdResourceTask);
    record.put("thirdQuartileDiskSpaceUsage", workload.thirdQuartileDiskSpaceUsage);
    record.put("thirdQuartileEnergy", workload.thirdQuartileEnergy);
    record.put("thirdQuartileMemory", workload.thirdQuartileMemory);
    record.put("thirdQuartileNetworkUsage", workload.thirdQuartileNetworkUsage);
    record.put("thirdQuartileResourceTask", workload.thirdQuartileResourceTask);
    record.put("totalResourceSeconds", workload.totalResourceSeconds);
    record.put("totalTasks", workload.totalTasks);
    record.put("totalWorkflows", workload.totalWorkflows);
    Long[] workflows;
    if (workload.workflows == null) {
      workflows = null;
    } else {
      workflows = (Long[])
          Arrays.stream(workload.workflows).map(x -> x.getId()).toArray();
    }
    record.put("workflows", workflows);
    record.put("workloadDescription", workload.workloadDescription);
    return record;
  }

  public static Schema getWorkloadSchema() {
    return workloadSchema;
  }
}
