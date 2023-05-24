package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.core.datasource.OperatingSystemDataSource;
import java.io.IOException;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Data source that uses Java's operating system MBean to gain access to resource information.
 * Adapted for the Apache Spark adapter.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
@AllArgsConstructor
public class SparkOperatingSystemDataSource extends OperatingSystemDataSource {

  /**
   * Data transfer object used for communicating with the driver.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Data
  public static class Dto implements Serializable {

    private static final long serialVersionUID = 4386177879327585527L;

    private final long committedVirtualMemorySize;

    private final long freePhysicalMemorySize;

    private final double processCpuLoad;

    private final long processCpuTime;

    private final long totalPhysicalMemorySize;

    private final int availableProcessors;

    private final double systemLoadAverage;

    private final String executorId;
  }

  private final PluginContext pluginContext;

  /**
   * Gathers the metrics the data source provides and sends them to the driver.
   * When a failure occurs when sending the metrics to the driver, an error is logged.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void gatherMetrics() {
    long vMemSize = getCommittedVirtualMemorySize();
    long freeMemSize = getFreePhysicalMemorySize();
    double cpuLoad = getProcessCpuLoad();
    long cpuTime = getProcessCpuTime();
    long totalMemSize = getTotalPhysicalMemorySize();
    int availableProc = getAvailableProcessors();
    double systemLoadAverage = getSystemLoadAverage();
    String executorId = pluginContext.executorID();
    Dto dto = new Dto(
        vMemSize, freeMemSize, cpuLoad, cpuTime, totalMemSize, availableProc, systemLoadAverage, executorId);
    try {
      pluginContext.send(dto);
    } catch (IOException e) {
      log.error("Something went wrong while trying to send a DTO to the driver.");
    }
  }
}
