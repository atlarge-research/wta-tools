package com.asml.apa.wta.core.model;

import com.asml.apa.wta.core.io.ParquetSchema;
import com.asml.apa.wta.core.model.enums.Domain;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

/**
 * Workload class corresponding to WTA format.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
 * @author Tianchen Qu
 * @since 1.0.0
 */
@Slf4j
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
  private final double firstQuartileMemory = -1.0;

  @Builder.Default
  private final double thirdQuartileMemory = -1.0;

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
  private final long medianNetworkUsage = -1L;

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
  private final double medianDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double firstQuartileDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double thirdQuartileDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double covDiskSpaceUsage = -1.0;

  @Builder.Default
  private final double minEnergy = -1.0;

  @Builder.Default
  private final double maxEnergy = -1.0;

  @Builder.Default
  private final double stdEnergy = -1.0;

  @Builder.Default
  private final double meanEnergy = -1.0;

  @Builder.Default
  private final double medianEnergy = -1.0;

  @Builder.Default
  private final double firstQuartileEnergy = -1.0;

  @Builder.Default
  private final double thirdQuartileEnergy = -1.0;

  @Builder.Default
  private final double covEnergy = -1.0;

  private final String workloadDescription;

  /**
   * This method should never be called as we do not need to output workloads in Parquet.
   *
   * @param schema schema The parquet schema
   * @throws RuntimeException always
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public final GenericRecord convertToRecord(ParquetSchema schema) {
    log.error(
        "The application attempted to convert a Workload to parquet, this is illegal and should never happen.");
    throw accessError();
  }

  /**
   * This method should never be called as we do not need to ever fetch its ID.
   *
   * @throws RuntimeException always
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public final long getId() {
    log.error("The application attempted to get the id of a Workload, this is illegal and should never happen.");
    throw accessError();
  }
}
