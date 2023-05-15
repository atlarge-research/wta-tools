package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import java.util.Arrays;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;
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

  @Setter
  private static String schemaVersion;

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

  private static Schema workloadSchema = SchemaBuilder.record("workload").namespace("com.asml.apa.wta.core.model")
          .fields()
          .name("workflows").type().nullable().array().items().longType().noDefault()
          .name("totalWorkflows").type().longType().noDefault()
          .name("totalTasks").type().longType().noDefault()
          .name("domain").type().stringType().noDefault()
          .name("startDate").type().longType().noDefault()
          .name("endDate").type().longType().noDefault()
          .name("numSites").type().longType().noDefault()
          .name("numResources").type().longType().noDefault()
          .name("numUsers").type().longType().noDefault()
          .name("numGroups").type().longType().noDefault()
          .name("totalResourceSeconds").type().doubleType().noDefault()
          .name("authors").type().nullable().array().items().nullable().stringType().noDefault()
          .name("minResourceTask").type().doubleType().noDefault()
          .name("maxResourceTask").type().doubleType().noDefault()
          .name("stdResourceTask").type().doubleType().noDefault()
          .name("meanResourceTask").type().doubleType().noDefault()
          .name("medianResourceTask").type().doubleType().noDefault()
          .name("firstQuartileResourceTask").type().doubleType().noDefault()
          .name("thirdQuartileResourceTask").type().doubleType().noDefault()
          .name("covResourceTask").type().doubleType().noDefault()
          .name("minMemory").type().doubleType().noDefault()
          .name("maxMeemory").type().doubleType().noDefault()
          .name("stdMemory").type().doubleType().noDefault()
          .name("meanMemory").type().doubleType().noDefault()
          .name("medianMemory").type().doubleType().noDefault()
          .name("firstQuartileMemory").type().longType().noDefault()
          .name("thirdQuartileMemory").type().longType().noDefault()
          .name("covMemory").type().doubleType().noDefault()
          .name("minNetworkUsage").type().longType().noDefault()
          .name("maxNetworkUsage").type().longType().noDefault()
          .name("stdNetworkUsage").type().doubleType().noDefault()
          .name("meanNetworkUsage").type().doubleType().noDefault()
          .name("medianNetworkUsage").type().doubleType().noDefault()
          .name("firstQuartileNetworkUsage").type().longType().noDefault()
          .name("thirdQuartileNetworkUsage").type().longType().noDefault()
          .name("covNetworkUsage").type().doubleType().noDefault()
          .name("minDiskSpaceUsage").type().doubleType().noDefault()
          .name("maxDiskSpaceUsage").type().doubleType().noDefault()
          .name("stdDiskSpaceUsage").type().doubleType().noDefault()
          .name("meanDiskSpaceUsage").type().doubleType().noDefault()
          .name("medianDiskSpaceUsage").type().longType().noDefault()
          .name("firstQuartileDiskSpaceUsage").type().longType().noDefault()
          .name("thirdQuartileDiskSpaceUsage").type().longType().noDefault()
          .name("covDiskSpaceUsage").type().doubleType().noDefault()
          .name("minEnergy").type().intType().noDefault()
          .name("maxEnergy").type().intType().noDefault()
          .name("stdEnergy").type().doubleType().noDefault()
          .name("meanEnergy").type().doubleType().noDefault()
          .name("medianEnergy").type().intType().noDefault()
          .name("firstQuartileEnergy").type().intType().noDefault()
          .name("thirdQuartileEnergy").type().intType().noDefault()
          .name("covEnergy").type().doubleType().noDefault()
          .name("workloadDescription").type().nullable().stringType().noDefault()
          .endRecord();

  public static GenericRecord convertWorkloadToRecord(Workload workload){
    GenericData.Record record = new GenericData.Record(workloadSchema);
    record.put("", workload.authors);
    record.put("", workload.covDiskSpaceUsage);
    record.put("", workload.covEnergy);
    record.put("", workload.covMemory);
    record.put("", workload.covNetworkUsage);
    record.put("", workload.covResourceTask);
    record.put("", workload.domain.getValue());
    record.put("", workload.endDate);
    record.put("", workload.firstQuartileDiskSpaceUsage);
    record.put("", workload.firstQuartileEnergy);
    record.put("", workload.firstQuartileMemory);
    record.put("", workload.firstQuartileNetworkUsage);
    record.put("", workload.firstQuartileResourceTask);
    record.put("", workload.maxDiskSpaceUsage);
    record.put("", workload.maxEnergy);
    record.put("", workload.maxMemory);
    record.put("", workload.maxResourceTask);
    record.put("", workload.maxNetworkUsage);
    record.put("", workload.meanDiskSpaceUsage);
    record.put("", workload.meanEnergy);
    record.put("", workload.meanMemory);
    record.put("", workload.meanNetworkUsage);
    record.put("", workload.meanResourceTask);
    record.put("", workload.medianDiskSpaceUsage);
    record.put("", workload.medianEnergy);
    record.put("", workload.medianMemory);
    record.put("", workload.medianNetworkUsage);
    record.put("", workload.medianResourceTask);
    record.put("", workload.minDiskSpaceUsage);
    record.put("", workload.minEnergy);
    record.put("", workload.minMemory);
    record.put("", workload.minNetworkUsage);
    record.put("", workload.minResourceTask);
    record.put("", workload.numGroups);
    record.put("", workload.numResources);
    record.put("", workload.numSites);
    record.put("", workload.numUsers);
    record.put("", workload.startDate);
    record.put("", workload.stdEnergy);
    record.put("", workload.stdDiskSpaceUsage);
    record.put("", workload.stdMemory);
    record.put("", workload.stdNetworkUsage);
    record.put("", workload.stdResourceTask);
    record.put("", workload.thirdQuartileDiskSpaceUsage);
    record.put("", workload.thirdQuartileEnergy);
    record.put("", workload.thirdQuartileMemory);
    record.put("", workload.thirdQuartileNetworkUsage);
    record.put("", workload.thirdQuartileResourceTask);
    record.put("", workload.totalResourceSeconds);
    record.put("", workload.totalTasks);
    record.put("", workload.totalWorkflows);
    record.put("", (Long[]) Arrays.stream(workload.workflows).map(x -> x.getId()).toArray());
    record.put("", workload.workloadDescription);
    return record;
  }

  public static Schema getWorkloadSchema(){
    return workloadSchema;
  }

}
