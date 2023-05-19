package com.asml.apa.wta.spark.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.asml.apa.wta.core.streams.KeyedStream;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Driver component of the plugin.
 *
 * @author Henry Page
 * @since 1.0.0
 */
public class WtaDriverPlugin implements DriverPlugin {

  /**
   * This method is called early in the initialization of the Spark driver.
   * Explicitly, it is called before the Spark driver's task scheduler is initialized. It is blocking.
   *
   * Expensive calls should be postponed or delegated to another thread.
   *
   * @param sCtx The current SparkContext.
   * @param pCtx Additional plugin-specific about the Spark application where the plugin is running.
   * @return Extra information provided to the executor
   */
  @Override
  public Map<String, String> init(SparkContext sCtx, PluginContext pCtx) {
    System.out.println("init");
    return new HashMap<>();
  }

  /**
   * Gets called just before shutdown. Recommended that no spark functions are used here.
   */
  @Override
  public void shutdown() {}

  @Override
  public Object receive(Object message) throws Exception {
    System.out.println(message.toString());
    return message;
  }
}
