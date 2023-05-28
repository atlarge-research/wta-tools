package com.asml.apa.wta.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Data transfer object for the {@link com.asml.apa.wta.core.datasource.OperatingSystemDataSource}.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
public class OperatingSystemDataSourceDto {

  private static final long serialVersionUID = 4386177879327585527L;

  private long committedVirtualMemorySize;

  private long freePhysicalMemorySize;

  private double processCpuLoad;

  private long processCpuTime;

  private long totalPhysicalMemorySize;

  private int availableProcessors;

  private double systemLoadAverage;
}
