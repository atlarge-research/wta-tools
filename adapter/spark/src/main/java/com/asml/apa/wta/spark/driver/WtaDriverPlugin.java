package com.asml.apa.wta.spark.driver;

import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.datasource.dto.IostatDataSourceDto;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
import com.asml.apa.wta.spark.streams.ResourceKey;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Driver component of the plugin.
 *
 * @author Henry Page and Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
public class WtaDriverPlugin implements DriverPlugin {

  private SparkContext sparkContext;

  @Getter
  private SparkDataSource sparkDataSource;

  private MetricStreamingEngine mse;

  /**
   * This method is called early in the initialization of the Spark driver.
   * Explicitly, it is called before the Spark driver's task scheduler is initialized. It is blocking.
   *
   * Expensive calls should be postponed or delegated to another thread.
   *
   * @param sparkCtx The current SparkContext.
   * @param pluginCtx Additional plugin-specific about the Spark application where the plugin is running.
   * @return Extra information provided to the executor
   * @author Henry Page and Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public Map<String, String> init(SparkContext sparkCtx, PluginContext pluginCtx) {
    this.sparkContext = sparkCtx;
    this.sparkDataSource = new SparkDataSource(this.sparkContext);
    initListeners();
    this.mse = new MetricStreamingEngine();
    return new HashMap<>();
  }

  /**
   * Gets called just before shutdown. Recommended that no spark functions are used here.
   *
   * @author Henry Page and Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public void shutdown() {
    mse.clearResourceStream();
  }

  /**
   * Gets called whenever a message is received from one of the executors.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public Object receive(Object message) {
    if (message instanceof IostatDataSourceDto) {
      mse.addToResourceStream(new ResourceKey(((IostatDataSourceDto)message).getExecutorId()), (IostatDataSourceDto)message);
    }
    return message;
  }

  /**
   * Initializes the listeners.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private void initListeners() {
    this.sparkDataSource.registerTaskListener();
    // register more listeners as needed
  }
}
