package com.asml.apa.wta.spark.streams;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Resource metrics record.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
@AllArgsConstructor
public class ResourceMetricsRecord implements Serializable {

  private static final long serialVersionUID = -3101218638564306099L;

  private final long committedVirtualMemorySize;

  private final long freePhysicalMemorySize;

  private final double processCpuLoad;

  private final long processCpuTime;

  private final long totalPhysicalMemorySize;

  private final int availableProcessors;

  private final double systemLoadAverage;
}
