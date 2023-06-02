package com.asml.apa.wta.core.dto;

import com.asml.apa.wta.core.supplier.OperatingSystemSupplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data transfer object for the {@link OperatingSystemSupplier}.
 *
 * @author Atour Mousavi Gourabi
 * @author Henry Page
 * @since 1.0.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OsInfoDto implements SupplierDto {

  private static final long serialVersionUID = 4386177879327585527L;

  private long committedVirtualMemorySize;

  private long freePhysicalMemorySize;

  private double processCpuLoad;

  private long processCpuTime;

  private long totalPhysicalMemorySize;

  private int availableProcessors;

  private double systemLoadAverage;

  private String architecture;
}
