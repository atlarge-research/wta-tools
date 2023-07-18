package com.asml.apa.wta.spark.driver;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.io.DiskOutputFile;
import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.dto.ResourceCollectionDto;
import com.asml.apa.wta.spark.stream.MetricStreamingEngine;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Driver component of the plugin. Only one instance of this class is initialized per Spark session.
 *
 * @author Pil Kyu Cho
 * @author Lohithsai Yadala Chanchu
 * @author Atour Mousavi Gourabi
 * @author Henry Page
 * @since 1.0.0
 */
@Getter
@Slf4j
public class WtaDriverPlugin implements DriverPlugin {

  private static final String TOOL_VERSION = "spark-wta-generator-1_0";

  private static final String WTA_VERSION = "schema-1.0";

  private MetricStreamingEngine metricStreamingEngine;

  private SparkDataSource sparkDataSource;

  private boolean error = false;

  /**
   * This method is called early in the initialization of the Spark driver.
   * Explicitly, it is called before the Spark driver's task scheduler is initialized. It is blocking.
   * Expensive calls should be postponed or delegated to another thread. If an error occurs while
   * initializing the plugin it will set {@link #error} to false.
   *
   * @param sparkCtx        current SparkContext
   * @param pluginCtx       additional plugin-specific about the Spark application where the plugin is running
   * @return                extra information provided to the executor
   * @since 1.0.0
   */
  @Override
  public Map<String, String> init(SparkContext sparkCtx, PluginContext pluginCtx) {
    log.info("Initialising WTA driver plugin.");
    Map<String, String> executorVars = new HashMap<>();
    try {
      String configFile = sparkCtx.conf()
          .get("spark.driver.extraJavaOptions")
          .split("-DconfigFile=")[1]
          .split(" ")[0];
      RuntimeConfig runtimeConfig = RuntimeConfig.readConfig(configFile);
      metricStreamingEngine = new MetricStreamingEngine();
      OutputFile outputFile = new DiskOutputFile(Path.of(runtimeConfig.getOutputPath()));
      String currentTime = String.valueOf(System.currentTimeMillis());
      WtaWriter wtaWriter = new WtaWriter(outputFile, WTA_VERSION, currentTime, TOOL_VERSION);
      sparkDataSource = new SparkDataSource(sparkCtx, runtimeConfig, metricStreamingEngine, wtaWriter);
      initListeners();
      executorVars.put("resourcePingInterval", String.valueOf(runtimeConfig.getResourcePingInterval()));
      executorVars.put(
          "executorSynchronizationInterval",
          String.valueOf(runtimeConfig.getExecutorSynchronizationInterval()));
      executorVars.put("errorStatus", "false");
    } catch (Exception e) {
      log.error("Error initialising WTA driver plugin, {} : {}.", e.getClass(), e.getMessage());
      executorVars.put("errorStatus", "true");
      error = true;
    }
    return executorVars;
  }

  /**
   * Receives messages from the executors.
   *
   * @param message       message that was sent by the executors, to be serializable
   * @return              response to the executor, if no response is expected the result is ignored
   * @since 1.0.0
   */
  @Override
  public Object receive(Object message) {
    log.trace("The driver received a message from an executor.");
    if (message instanceof ResourceCollectionDto) {
      ((ResourceCollectionDto) message)
          .getResourceCollection()
          .forEach(r -> metricStreamingEngine.addToResourceStream(r.getExecutorId(), r));
    }
    return null;
  }

  /**
   * Gets called just before shutdown. If an error occurred, it is logged before shutdown
   * Recommended that no Spark functions are used here
   *
   * @since 1.0.0
   */
  @Override
  public void shutdown() {
    if (error) {
      log.error("Plugin shutting down without generating files.");
    } else {
      log.info("Plugin shutting down successfully.");
    }
  }

  /**
   * Initializes the listeners to get Spark metrics.
   *
   * @since 1.0.0
   */
  public void initListeners() {
    log.trace("Initializing listeners.");
    if (!sparkDataSource.getRuntimeConfig().isStageLevel()) {
      this.sparkDataSource.registerTaskListener();
      log.info("Task level metrics are enabled.");
    } else {
      log.info("Stage level metrics are enabled.");
    }
    sparkDataSource.registerStageListener();
    sparkDataSource.registerJobListener();
    sparkDataSource.registerApplicationListener();
  }
}
