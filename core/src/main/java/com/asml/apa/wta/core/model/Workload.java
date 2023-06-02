package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.model.enums.Domain;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

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
  private final long minNetworkUsage = -1L;

  @Builder.Default
  private final long maxNetworkUsage = -1L;

  @Builder.Default
  private final double stdNetworkUsage = -1.0;

  @Builder.Default
  private final double meanNetworkUsage = -1.0;

  @Builder.Default
  private final double medianNetworkUsage = -1.0;

  @Builder.Default
  private final long firstQuartileNetworkUsage = -1L;

  @Builder.Default
  private final long thirdQuartileNetworkUsage = -1L;

  @Builder.Default
  private final double covNetworkUsage = -1.0;

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

  /**
   * This method shouldn't be called as it will be output into json file that doesn't require conversion to Record.
   * @param checker checker
   * @param schema schema
   * @throws RuntimeException exception since this shouldn't be called
   */
  @Override
  public GenericRecord convertToRecord(Boolean[] checker, Schema schema) {
    throw new RuntimeException("Something went wrong, this method shouldn't be called!");
  }
}
