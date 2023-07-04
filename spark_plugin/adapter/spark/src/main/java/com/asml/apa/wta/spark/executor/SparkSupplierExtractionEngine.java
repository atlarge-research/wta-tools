package com.asml.apa.wta.spark.executor;

import com.asml.apa.wta.core.dto.BaseSupplierDto;
import com.asml.apa.wta.core.supplier.SupplierExtractionEngine;
import com.asml.apa.wta.spark.dto.ResourceCollectionDto;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.plugin.PluginContext;

/**
 * Extracts resource utilization information whilst augmenting it with Spark information.
 *
 * @author Henry Page
 * @since 1.0.0
 */
@Slf4j
public class SparkSupplierExtractionEngine extends SupplierExtractionEngine<SparkBaseSupplierWrapperDto> {

  private final PluginContext pluginContext;

  private final int executorSynchronizationInterval;

  private final ScheduledExecutorService bufferSynchronizer = Executors.newScheduledThreadPool(1);

  /**
   * Specialised extraction engine for Spark.
   *
   * @param resourcePingInterval                  how often to ping the suppliers, in milliseconds
   * @param pluginContext                         plugin contex
   * @param executorSynchronizationInterval       how often to send the buffer, in milliseconds
   * @since 1.0.0
   */
  public SparkSupplierExtractionEngine(
      int resourcePingInterval, PluginContext pluginContext, int executorSynchronizationInterval) {
    super(resourcePingInterval);
    this.pluginContext = pluginContext;
    this.executorSynchronizationInterval = executorSynchronizationInterval;
  }

  /**
   * Overridden method to ping the resource and buffer the result. If a non-positive executor
   * synchronization interval is set, the result is sent immediately.
   *
   * @return        {@link CompletableFuture} representing the result of the ping and buffer operation
   * @since 1.0.0
   */
  @Override
  public CompletableFuture<Void> pingAndBuffer() {
    log.trace(pluginContext.executorID() + " is pinging suppliers.");
    return ping().thenAcceptAsync(result -> {
      if (this.executorSynchronizationInterval <= 0) {
        sendBuffer(List.of(result));
      } else {
        getBuffer().add(result);
      }
    });
  }

  /**
   * This method gets called by the scheduler to send the resource buffer of the extraction engine.
   *
   * @param snapshots         snapshots to send to the buffer
   * @since 1.0.0
   */
  private void sendBuffer(List<SparkBaseSupplierWrapperDto> snapshots) {
    ResourceCollectionDto bufferSnapshot = new ResourceCollectionDto(snapshots);
    if (bufferSnapshot.getResourceCollection().isEmpty()) {
      log.trace(pluginContext.executorID() + " has no buffer to send. Aborting send.");
      return;
    }
    try {
      log.trace(pluginContext.executorID() + " is sending buffer {}.", bufferSnapshot);
      this.pluginContext.send(bufferSnapshot);
    } catch (IOException e) {
      log.error("Failed to send buffer: {}.", bufferSnapshot, e);
    }
  }

  /**
   * This method gets called by the scheduler to send the resource buffer of the extraction engine.
   *
   * @since 1.0.0
   */
  private void sendBuffer() {
    sendBuffer(getAndClear());
  }

  /**
   * Scheduled task to send the resource buffer of the extraction engine. If the
   * {@link #executorSynchronizationInterval} is set to a non-positive value, resource information gets
   * sent immediately.
   */
  public void startSynchonizing() {
    if (this.executorSynchronizationInterval > 0) {
      log.trace(pluginContext.executorID() + " is starting to synchronize.");
      this.bufferSynchronizer.scheduleAtFixedRate(
          this::sendBuffer, 0, executorSynchronizationInterval, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Stops the synchronizer from sending information to the driver.
   *
   * @since 1.0.0
   */
  public void stopSynchronizing() {
    log.trace(pluginContext.executorID() + " is stopping to synchronize.");
    this.bufferSynchronizer.shutdown();
  }

  /**
   * Augments the base supplier Dto with Spark information.
   *
   * @param record        {@link BaseSupplierDto} to transform
   * @return              {@link SparkBaseSupplierWrapperDto} containing information pertaining to Spark
   * @since 1.0.0
   */
  @Override
  public SparkBaseSupplierWrapperDto transform(BaseSupplierDto record) {
    return SparkBaseSupplierWrapperDto.builder()
        .executorId(pluginContext.executorID())
        .timestamp(record.getTimestamp())
        .osInfoDto(record.getOsInfoDto())
        .iostatDto(record.getIostatDto())
        .dstatDto(record.getDstatDto())
        .perfDto(record.getPerfDto())
        .jvmFileDto(record.getJvmFileDto())
        .procDto(record.getProcDto())
        .build();
  }
}
