package com.asml.apa.wta.spark.driver;

import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.datasource.SparkOperatingSystemDataSource;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
import com.asml.apa.wta.spark.streams.ResourceKey;
import com.asml.apa.wta.spark.streams.ResourceMetricsRecord;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import java.util.UUID;

import com.asml.apa.wta.core.streams.KeyedStream;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Driver component of the plugin.
 *
 * @author Atour Mousavi Gourabi
 * @author Henry Page
 * @since 1.0.0
 */
public class WtaDriverPlugin implements DriverPlugin {

  private SparkContext sparkContext;

  @Getter
  private SparkDataSource sparkDataSource;

  private MetricStreamingEngine streamingEngine;

  /**
   * This method is called early in the initialization of the Spark driver.
   * Explicitly, it is called before the Spark driver's task scheduler is initialized. It is blocking.
   *
   * Expensive calls should be postponed or delegated to another thread.
   *
   * @param sparkCtx The current SparkContext.
   * @param pluginCtx Additional plugin-specific about the Spark application where the plugin is running.
   * @return Extra information provided to the executor
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public Map<String, String> init(SparkContext sparkCtx, PluginContext pluginCtx) {
    sparkContext = sparkCtx;
    sparkDataSource = new SparkDataSource(sparkContext);
    streamingEngine = new MetricStreamingEngine();
    return new HashMap<>();
  }

  /**
   * Receives messages from the executors.
   *
   * @param message the message that was sent by the executors, to be serializable
   * @return a response to the executor, if no response is expected the result is ignored
   * @author Atour Mousavi Gourabi
   */
  @Override
  public Object receive(Object message) {
    if (message instanceof SparkOperatingSystemDataSource.Dto) {
      SparkOperatingSystemDataSource.Dto dto = (SparkOperatingSystemDataSource.Dto) message;
      ResourceKey resourceKey = new ResourceKey(dto.getExecutorId());
      ResourceMetricsRecord resourceRecord = new ResourceMetricsRecord(
          dto.getCommittedVirtualMemorySize(),
          dto.getFreePhysicalMemorySize(),
          dto.getProcessCpuLoad(),
          dto.getProcessCpuTime(),
          dto.getTotalPhysicalMemorySize(),
          dto.getAvailableProcessors(),
          dto.getSystemLoadAverage());
      streamingEngine.addToResourceStream(resourceKey, resourceRecord);
    }
    return null;
  }

  /**
   * Gets called just before shutdown. Recommended that no spark functions are used here.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void shutdown() {
    // clean up
  }
}
