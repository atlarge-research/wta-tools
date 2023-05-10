package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

/**
 * Workflow class corresponding to WTA format.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class Workflow extends SchemaObject {

  private static final long serialVersionUID = 2L;

  // Current assumption is that schemaVersion is the same across all Workflow objects, but it might be different
  // across different SchemaObjects
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
