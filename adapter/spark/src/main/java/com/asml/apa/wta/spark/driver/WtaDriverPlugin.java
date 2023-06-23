package com.asml.apa.wta.spark.driver;

import com.asml.apa.wta.core.WtaWriter;
import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.io.DiskOutputFile;
import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.core.model.ResourceState;
import com.asml.apa.wta.core.model.Task;
import com.asml.apa.wta.core.model.Workflow;
import com.asml.apa.wta.core.model.Workload;
import com.asml.apa.wta.core.streams.Stream;
import com.asml.apa.wta.spark.datasource.SparkDataSource;
import com.asml.apa.wta.spark.dto.ResourceAndStateWrapper;
import com.asml.apa.wta.spark.dto.ResourceCollectionDto;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  private MetricStreamingEngine metricStreamingEngine;

  private SparkDataSource sparkDataSource;

  private OutputFile outputFile;

  private boolean error = false;

  /**
   * This method is called early in the initialization of the Spark driver.
   * Explicitly, it is called before the Spark driver's task scheduler is initialized. It is blocking.
   * Expensive calls should be postponed or delegated to another thread. If an error occurs while
   * initializing the plugin, the plugin should call {@link #shutdown()} with {@link #error} set to false.
   *
   * @param sparkCtx The current SparkContext.
   * @param pluginCtx Additional plugin-specific about the Spark application where the plugin is running.
   * @return Extra information provided to the executor
   * @author Pil Kyu Cho
   * @author Henry Page
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
      this.metricStreamingEngine = new MetricStreamingEngine();
      sparkDataSource = new SparkDataSource(sparkCtx, runtimeConfig);
      outputFile = new DiskOutputFile(Path.of(runtimeConfig.getOutputPath()));
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
   * @param message the message that was sent by the executors, to be serializable
   * @return a response to the executor, if no response is expected the result is ignored
   * @author Atour Mousavi Gourabi
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
   * Gets called just before shutdown. If no prior error occurred, it collects all the
   * tasks, workflows, and workloads from the Spark job and writes them to a parquet file.
   * Otherwise, logs the error and just shuts down.
   * Recommended that no Spark functions are used here.
   *
   * @author Pil Kyu Cho
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void shutdown() {
    if (error) {
      log.error("Plugin shutting down without generating files");
      return;
    }
    try {
      endApplicationAndWrite();
    } catch (Exception e) {
      log.error("A {} error occurred while generating files: {}", e.getClass(), e.getMessage());
    }
    log.info("WTA driver plugin shutting down.");
  }

  /**
   * Removes the listeners and writes the collected data to the output file.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  private void endApplicationAndWrite() {
    log.debug("Attempting to write data to file.");
    removeListeners();
    List<Task> tasks = sparkDataSource.getRuntimeConfig().isStageLevel()
        ? sparkDataSource.getStageLevelListener().getProcessedObjects()
        : sparkDataSource.getTaskLevelListener().getProcessedObjects();
    List<Workflow> workflows = sparkDataSource.getJobLevelListener().getProcessedObjects();
    List<ResourceAndStateWrapper> resourceAndStateWrappers = metricStreamingEngine.collectResourceInformation();
    List<Resource> resources = resourceAndStateWrappers.stream()
        .map(ResourceAndStateWrapper::getResource)
        .collect(Collectors.toList());
    List<ResourceState> resourceStates = resourceAndStateWrappers.stream()
        .flatMap(rs -> rs.getStates().stream())
        .collect(Collectors.toList());
    Workload workload = sparkDataSource
        .getApplicationLevelListener()
        .getProcessedObjects()
        .get(0);
    WtaWriter wtaWriter = new WtaWriter(outputFile, "schema-1.0", TOOL_VERSION);
    wtaWriter.write(Task.class, tasks);
    wtaWriter.write(Resource.class, resources);
    wtaWriter.write(Workflow.class, workflows);
    wtaWriter.write(ResourceState.class, resourceStates);
    wtaWriter.write(workload);

    Stream.deleteAllSerializedFiles();
    log.debug("Successfully wrote data to file.");
  }

  /**
   * Initializes the listeners to get Spark metrics.
   *
   * @author Lohithsai Yadala Chanchu
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
    this.sparkDataSource.registerStageListener();
    this.sparkDataSource.registerJobListener();
    this.sparkDataSource.registerApplicationListener();
  }

  /**
   * Removes the listeners.
   *
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public void removeListeners() {
    log.trace("Removing listeners.");
    sparkDataSource.removeTaskListener();
    sparkDataSource.removeStageListener();
    sparkDataSource.removeJobListener();
    sparkDataSource.removeApplicationListener();
  }
}
