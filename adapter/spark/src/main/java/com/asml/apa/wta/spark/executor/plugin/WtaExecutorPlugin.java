package com.asml.apa.wta.spark.executor.plugin;

import com.asml.apa.wta.spark.WtaPlugin;
import com.asml.apa.wta.spark.driver.WtaDriverPlugin;
import com.asml.apa.wta.spark.executor.engine.SparkSupplierExtractionEngine;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskFailedReason;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Executor component of the plugin.
 *
 * @author Henry Page
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Getter
@Slf4j
public class WtaExecutorPlugin implements ExecutorPlugin {

  private SparkSupplierExtractionEngine supplierEngine;

  private boolean error = false;

  /**
   * This method is called when the plugin is initialized on the executor.
   * Developers are urged not to put inefficient code here as it blocks executor initialization until
   * it is completed.
   *
   * @param pCtx The PluginContext object that represents the context of the plugin.
   * @param extraConf A map object that contains any extra configuration information. This map
   *                  is directly returned from {@link WtaDriverPlugin#init(SparkContext, PluginContext)}
   * @see WtaPlugin#executorPlugin() where a new instance of the plugin is created. This gets called as soon
   * as it is loaded on to the executor.
   *
   * @author Henry Page
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @Override
  public void init(PluginContext pCtx, Map<String, String> extraConf) {
    if (extraConf == null
        || extraConf.isEmpty()
        || !extraConf.containsKey("errorStatus")
        || extraConf.get("errorStatus").equals("true")) {
      log.error("Error initialising WTA executor plugin due to driver failure.");
      error = true;
      return;
    }
    try {
      int resourcePingInterval = Integer.parseInt(extraConf.get("resourcePingInterval"));
      int executorSynchronizationInterval = Integer.parseInt(extraConf.get("executorSynchronizationInterval"));
      supplierEngine =
          new SparkSupplierExtractionEngine(resourcePingInterval, pCtx, executorSynchronizationInterval);
      supplierEngine.startPinging();
      supplierEngine.startSynchonizing();
    } catch (NumberFormatException e) {
      log.error("Invalid resource ping interval or executor synchronization interval.");
      error = true;
    } catch (Exception e) {
      log.error("Error pinging resources or synchronizing executor and driver.");
      error = true;
    }
  }

  /**
   * Called before task is started.
   * Developers should note that expensive operations should be avoided, since it gets called on every task.
   * Exceptions thrown here are not propagated, meaning a task won't fail if this method throws an exception.
   * <a href="https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/api/plugin/ExecutorPlugin.html#init-org.apache.spark.api.plugin.PluginContext-java.util.Map-">Refer to the docs</a> for more information.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onTaskStart() {}

  /**
   * Gets called when a task is successfully completed.
   * Gets called even if {@link #onTaskStart()} threw an exception.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onTaskSucceeded() {}

  /**
   * Gets called if a task fails.
   *
   * @param failureReason The reason the task failed, accessible through a string.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void onTaskFailed(TaskFailedReason failureReason) {
    log.error("A task has failed due to {}.", failureReason.toErrorString());
  }

  /**
   * Gets called just before shutdown. Blocks executor shutdown until it is completed.
   *
   * @author Henry Page
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @Override
  public void shutdown() {
    if (error) {
      log.error("Shutting down WTA executor plugin due to driver failure.");
      return;
    }
    supplierEngine.stopPinging();
    supplierEngine.stopSynchronizing();
    log.info("Shutting down WTA executor with no error.");
  }
}
