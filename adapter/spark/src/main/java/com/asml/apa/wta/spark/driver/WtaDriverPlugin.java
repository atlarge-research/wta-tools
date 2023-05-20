package com.asml.apa.wta.spark.driver;

import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.util.HashMap;
import java.util.Map;
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

  private SparkContext sparkContext;

  private SparkDataSource sparkDataSource;

  private Workload.WorkloadBuilder workloadBuilder;

  /**
   * This method is called early in the initialization of the Spark driver.
   * Explicitly, it is called before the Spark driver's task scheduler is initialized. It is blocking.
   *
   * Expensive calls should be postponed or delegated to another thread.
   *
   * @param sCtx The current SparkContext.
   * @param pCtx Additional plugin-specific about the Spark application where the plugin is running.
   * @return Extra information provided to the executor
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public Map<String, String> init(SparkContext sCtx, PluginContext pCtx) {
    this.sparkContext = sCtx;
    this.sparkDataSource = new SparkDataSource(sparkContext);
    this.workloadBuilder = Workload.builder().startDate(sparkContext.startTime());
    return new HashMap<>();
  }

  /**
   * Gets called just before shutdown. Recommended that no spark functions are used here.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void shutdown() {
    this.sparkContext = null; // release resource
  }
}
