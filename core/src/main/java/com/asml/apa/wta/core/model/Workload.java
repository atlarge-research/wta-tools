package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

/**
 * Workload class corresponding to WTA format.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
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

  private final long dateStart;

  private final long dateEnd;

  private final long numSites;

  private final long numResources;

  private final long numUsers;

  private final long numGroups;

  private final double totalResourceSeconds;

  private final String[] authors;

  @Builder.Default
  private final double minResourceTask = -1.0;

  @Builder.Default
  private final double maxResourceTask = -1.0;

  @Builder.Default
  private final double stdResourceTask = -1.0;

  @Builder.Default
  private final double meanResourceTask = -1.0;

  @Builder.Default
  private final double medianResourceTask = -1.0;

  @Builder.Default
  private final double firstQuartileResourceTask = -1.0;

  @Builder.Default
  private final double thirdQuartileResourceTask = -1.0;

  @Builder.Default
  private final double covResourceTask = -1.0;

  @Builder.Default
  private final double minMemory = -1.0;

  @Builder.Default
  private final double maxMemory = -1.0;

  @Builder.Default
  private final double stdMemory = -1.0;

  @Builder.Default
  private final double meanMemory = -1.0;

  @Builder.Default
  private final double medianMemory = -1.0;

  @Builder.Default
  private final long firstQuartileMemory = -1L;

  @Builder.Default
  private final long thirdQuartileMemory = -1L;

  @Builder.Default
  private final double covMemory = -1.0;

  @Builder.Default
  private final long minNetworkIoTime = -1L;

  @Builder.Default
  private final long maxNetworkIoTime = -1L;

  @Builder.Default
  private final double stdNetworkIoTime = -1.0;

  @Builder.Default
  private final double meanNetworkIoTime = -1.0;

  @Builder.Default
  private final double medianNetworkIoTime = -1.0;

  @Builder.Default
  private final long firstQuartileNetworkIoTime = -1L;

  @Builder.Default
  private final long thirdQuartileNetworkIoTime = -1L;

  @Builder.Default
  private final double covNetworkIoTime = -1.0;

  @Builder.Default
  private final double minDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double maxDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double stdDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double meanDiskSpaceUsage = -1.0;

  @Builder.Default
  private final long medianDiskSpaceUsage = -1L;

  @Builder.Default
  private final long firstQuartileDiskSpaceUsage = -1L;

  @Builder.Default
  private final long thirdQuartileDiskSpaceUsage = -1L;

  @Builder.Default
  private final double covDiskSpaceUsage = -1.0;

  @Builder.Default
  private final int minEnergy = -1;

  @Builder.Default
  private final int maxEnergy = -1;

  @Builder.Default
  private final double stdEnergy = -1.0;

  @Builder.Default
  private final double meanEnergy = -1.0;

  @Builder.Default
  private final int medianEnergy = -1;

  @Builder.Default
  private final int firstQuartileEnergy = -1;

  @Builder.Default
  private final int thirdQuartileEnergy = -1;

  @Builder.Default
  private final double covEnergy = -1.0;

  private final String workloadDescription;
}
