package com.asml.apa.wta.core.WTAClasses;

import com.asml.apa.wta.core.WTAClasses.enums.Domain;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
public class Workflow {
  private final String version;
  private final long workflow_id;
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
}
