package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;

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
}
