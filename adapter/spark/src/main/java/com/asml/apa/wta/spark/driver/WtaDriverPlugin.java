package com.asml.apa.wta.spark.driver;

import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.spark.datasource.IostatDataSource;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import java.util.UUID;
import java.util.concurrent.*;

import com.asml.apa.wta.core.streams.KeyedStream;
import com.asml.apa.wta.spark.listener.AbstractListener;
import com.asml.apa.wta.spark.listener.TaskLevelListener;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
import com.asml.apa.wta.spark.streams.ResourceKey;
import com.asml.apa.wta.spark.streams.ResourceMetricsRecord;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.sparkproject.jetty.util.IO;

/**
 * Driver component of the plugin.
 *
 * @author Henry Page
 * @since 1.0.0
 */
public class WtaDriverPlugin implements DriverPlugin {

  private SparkContext sparkContext;

  @Getter
  private SparkDataSource sparkDataSource;

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
    this.sparkContext = sparkCtx;
    this.sparkDataSource = new SparkDataSource(this.sparkContext);
    this.sparkDataSource.registerTaskListener();
    return new HashMap<>();
  }

  /**
   * Gets called just before shutdown. Recommended that no spark functions are used here.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void shutdown() {}

  @Override
  public Object receive(Object message) {
    System.out.println(message.toString());
    IostatDataSource k = (IostatDataSource) message;

    MetricStreamingEngine mse = new MetricStreamingEngine();

    mse.addToResourceStream(new ResourceKey(k.getExecutorId()), new ResourceMetricsRecord(k));
    return message;
  }
}
