package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import lombok.*;

/**
 * Workflow class
 *
 * @author Lohithsai Yadala Chanchu
 * @version 1.0.0
 */
@Data
@Builder
@AllArgsConstructor
public class Workflow {
  @Setter
  private static String schemaVersion;

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
}
