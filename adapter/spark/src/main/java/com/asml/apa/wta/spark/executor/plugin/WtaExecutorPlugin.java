package com.asml.apa.wta.spark.executor.plugin;

import com.asml.apa.wta.spark.WtaPlugin;
import com.asml.apa.wta.spark.driver.WtaDriverPlugin;
import com.asml.apa.wta.spark.dto.ResourceCollectionDto;
import com.asml.apa.wta.spark.executor.engine.SparkSupplierExtractionEngine;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
@Slf4j
public class WtaExecutorPlugin implements ExecutorPlugin {

  private PluginContext pluginContext;

  private SparkSupplierExtractionEngine supplierEngine;

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

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
   * @since 1.0.0
   */
  @Override
  public void init(PluginContext pCtx, Map<String, String> extraConf) {
    int resourcePingInterval = extraConf.get("resourcePingInterval") == null
        ? 1000
        : Integer.parseInt(extraConf.get("resourcePingInterval"));
    int executorSynchronizationInterval = extraConf.get("executorSynchronizationInterval") == null
        ? 2000
        : Integer.parseInt(extraConf.get("executorSynchronizationInterval"));
    this.pluginContext = pCtx;
    this.supplierEngine = new SparkSupplierExtractionEngine(pluginContext);
    this.supplierEngine.startPinging(resourcePingInterval);
    this.startSending(executorSynchronizationInterval);
  }

  /**
   * Scheduled task to send the resource buffer of the extraction engine, with an initial delay
   * of 1 second and a fixed rate of 5 seconds.
   *
   * @param executorSynchronizationInterval How often to synchronize the buffer in milliseconds
   * @author Henry Page
   * @since 1.0.0
   */
  private void startSending(int executorSynchronizationInterval) {
    this.scheduler.scheduleAtFixedRate(
        this::sendBuffer, 1000, executorSynchronizationInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * This method gets called by the scheduler to send the resource buffer of the extraction engine.
   * This happens every 5 seconds.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  private void sendBuffer() {
    ResourceCollectionDto bufferSnapshot = new ResourceCollectionDto(this.supplierEngine.getAndClear());
    if (bufferSnapshot.getResourceCollection().isEmpty()) {
      return;
    }
    try {
      this.pluginContext.send(bufferSnapshot);
    } catch (IOException e) {
      log.error("Failed to send buffer: ", bufferSnapshot, e);
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
  public void onTaskFailed(TaskFailedReason failureReason) {}

  /**
   * Gets called just before shutdown. Blocks executor shutdown until it is completed.
   *
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public void shutdown() {
    this.scheduler.shutdown();
    this.supplierEngine.stopPinging();
  }
}
