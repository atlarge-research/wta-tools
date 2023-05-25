package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.core.datasource.OperatingSystemDataSource;
import com.asml.apa.wta.spark.dto.SparkOperatingSystemDataSourceDto;
import java.io.IOException;
import lombok.AllArgsConstructor;
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
    SparkOperatingSystemDataSourceDto dto = new SparkOperatingSystemDataSourceDto(
        vMemSize, freeMemSize, cpuLoad, cpuTime, totalMemSize, availableProc, systemLoadAverage, executorId);
    try {
      pluginContext.send(dto);
    } catch (IOException e) {
      log.error("Something went wrong while trying to send a DTO to the driver.");
    }
  }
}
