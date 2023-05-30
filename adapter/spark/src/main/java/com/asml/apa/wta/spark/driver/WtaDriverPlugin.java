package com.asml.apa.wta.spark.driver;

import com.asml.apa.wta.spark.datasource.SparkDataSource;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Driver component of the plugin.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
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
    sparkDataSource = new SparkDataSource(this.sparkContext);
    initListeners();
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
    return null;
  }

  /**
   * Gets called just before shutdown. Recommended that no spark functions are used here.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void shutdown() {}

  /**
   * Initializes the listeners.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private void initListeners() {
  if(sparkDataSource.getRuntimeConfig().isStageLevel()) {
    this.sparkDataSource.registerStageListener();
  }
  else {
    this.sparkDataSource.registerTaskListener();
  }
    // register more listeners as needed
  }
}
