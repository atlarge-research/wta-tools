package com.asml.apa.wta.spark.listener;

import com.asml.apa.wta.core.config.RuntimeConfig;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.OsInfoDto;
import com.asml.apa.wta.core.model.Resource;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import com.asml.apa.wta.spark.streams.MetricStreamingEngine;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;

/**
 * ResourceLevelListener that is responsible for aggregating resource-level events relating to the lifecycle of an executor.
 */
public class ExecutorResourceListener extends AbstractListener<Resource> {

  private final MetricStreamingEngine metricStreamingEngine;

  public ExecutorResourceListener(
      SparkContext sparkContext, RuntimeConfig config, MetricStreamingEngine metricStreamingEngine) {
    super(sparkContext, config);
    this.metricStreamingEngine = metricStreamingEngine;
  }

  /**
   * Callback for when an executor is removed from the cluster. An executor can be removed from the cluster for various reasons.
   *
   * @param executorRemoved object that provides details about the executor that was removed
   */
  @Override
  public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
    final String id = executorRemoved.executorId();
    final String type = "cluster node";
    final List<SparkBaseSupplierWrapperDto> infoList =
        metricStreamingEngine.getResourceStream().onKey(id).toList();
    final List<OsInfoDto> iostatDtos = infoList.stream().map(SparkBaseSupplierWrapperDto::getOsInfoDto).collect(Collectors.toList());
    final String os = getConstantInfo(iostatDtos, OsInfoDto::getOs, "Unknown");
    final String procModel = getConstantInfo(iostatDtos, OsInfoDto::getArchitecture, "Unknown");
    final long memory = getConstantInfo(iostatDtos, OsInfoDto::getTotalPhysicalMemorySize, -1L);






  }


  /**
   * Gets information that is supposed to be constant across a stream of pings.
   *
   * @param <T> The type of object that we need to extract information from
   * @param <R> The type of information that we need to extract
   * @param infoList A property of the {@link SparkBaseSupplierWrapperDto} that needs to be tested for
   * @param mapper A function that transforms an instance of T -> R
   * @param defaultValue The default value to return if no value is found (when the supplier is unavailable)
   * @return The first value found
   */
  private static <T,R> R getConstantInfo(List<T> infoList, Function<T,R> mapper, R defaultValue) {
    return infoList.stream().filter(Objects::nonNull).map(mapper).distinct().findFirst().orElse(defaultValue);
  }
}
