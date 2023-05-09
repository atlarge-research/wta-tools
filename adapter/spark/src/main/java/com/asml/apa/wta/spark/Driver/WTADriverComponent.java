package com.asml.apa.wta.spark.Driver;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Driver component of the plugin.
 *
 * @author Henry Page
 * @version 1.0.0
 */
public class WTADriverComponent implements DriverPlugin {

  /**
   * This method is called early in the initialization of the Spark driver.
   * Explicitly, it is called before the Spark driver's task scheduler is initialized. It is blocking.
   *
   * Expensive calls should be postponed or delegated to another thread.
   *
   * @param sCtx The current SparkContext.
   * @param pCtx Additional plugin-specific about the Spark application where the plugin is running.
   * @return Extra information provided to the executor.
   */
  public Map<String, String> init(SparkContext sCtx, PluginContext pCtx) {
    return new HashMap<>();
  }

  /**
   * Gets called just before shutdown. Recommended that no spark functions are used here.
   */
  @Override
  public void shutdown() {}
}
