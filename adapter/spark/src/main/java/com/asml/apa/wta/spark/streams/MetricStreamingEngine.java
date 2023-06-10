package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.streams.KeyedStream;
import com.asml.apa.wta.spark.dto.SparkBaseSupplierWrapperDto;
import lombok.Getter;

/**
 * Facade that maintains the resource and task streams.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Getter
public class MetricStreamingEngine {

  private final KeyedStream<String, SparkBaseSupplierWrapperDto> executorResourceStream;

  private final KeyedStream<TaskKey, TaskMetricsRecord> taskStream;

  /**
   * Initializes the streams.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public MetricStreamingEngine() {
    executorResourceStream = new KeyedStream<>();
    taskStream = new KeyedStream<>();
  }

  /**
   * Adds resource metrics to the resource stream.
   *
   * @param resourceKey A {@link String} identifying the resource. This is usually the executorID.
   * @param record the {@link com.asml.apa.wta.spark.datasource.SparkDataSource} containing metrics.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToResourceStream(String resourceKey, SparkBaseSupplierWrapperDto record) {
    executorResourceStream.addToStream(resourceKey, record);
  }

  /**
   * Adds task metrics to the task stream.
   *
   * @param task the {@link com.asml.apa.wta.spark.streams.TaskKey} of the task
   * @param record the {@link TaskMetricsRecord} containing the metrics
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToTaskStream(TaskKey task, TaskMetricsRecord record) {
    taskStream.addToStream(task, record);
  }

  //  /**
  //   * Consumes all resource related streams and aggregates them into objects.
  //   *
  //   * @return a list of {@link ResourceAndStateWrapper} objects containing the resource and its states
  //   * @author Henry Page
  //   * @since 1.0.0
  //   */
  //  public List<ResourceAndStateWrapper> processResources() {
  //    Map<String, List<SparkBaseSupplierWrapperDto>> result = executorResourceStream.collectAll();
  //
  //    result.entrySet().stream().map(entry -> {
  //      Resource resource = produceResource(entry.getKey(), entry.getValue());
  //
  //    })
  //
  //
  //
  //
  //  }
  //
  //  private Resource produceResource(String key, List<SparkBaseSupplierWrapperDto> value) {
  //    final long resourceId = Math.abs(key.hashCode());
  //    final String type = "cluster node";
  //
  //
  //
  //  }
  //
  //
  //  /**
  //   * Gets information that is supposed to be constant across a stream of pings.
  //   *
  //   * @param <T> The type of object that we need to extract information from
  //   * @param <R> The type of information that we need to extract
  //   * @param infoList A property of the {@link SparkBaseSupplierWrapperDto} that needs to be tested for
  //   * @param mapper A function that transforms an instance of T -> R
  //   * @param defaultValue The default value to return if no value is found (when the supplier is unavailable)
  //   * @return The first value found
  //   */
  //  private static <T, R> R getConstantInfo(List<T> infoList, Function<T, R> mapper, R defaultValue) {
  //    return infoList.stream()
  //            .filter(Objects::nonNull)
  //            .map(mapper)
  //            .distinct()
  //            .findFirst()
  //            .orElse(defaultValue);
  //  }

}
