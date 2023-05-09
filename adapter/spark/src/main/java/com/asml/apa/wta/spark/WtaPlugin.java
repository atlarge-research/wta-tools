package com.asml.apa.wta.spark;

import com.asml.apa.wta.spark.Driver.WtaDriverPlugin;
import com.asml.apa.wta.spark.Executor.WtaExecutorPlugin;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/**
 * The WTA plugin is a Spark plugin that provides a way to convert spark execution information
 * into WTA format. Refer to the <a href="https://wta.atlarge-research.com/">Workflow Trace Archive</a> for more information.
 * It includes driver- and executor-side plugins that can be loaded when the plugin is used.
 *
 * @author Henry Page
 * @since 1.0.0
 */
public class WtaPlugin implements SparkPlugin {

  /**
   * Returns the driver-side component of the plugin.
   *
   * @return The driver-side component initialised at startup.
   */
  @Override
  public DriverPlugin driverPlugin() {
    return new WtaDriverPlugin();
  }

  /**
   * Returns the executor-side component of the plugin.
   *
   * @return The executor-side component initialised at startup.
   */
  @Override
  public ExecutorPlugin executorPlugin() {
    return new WtaExecutorPlugin();
  }
}
