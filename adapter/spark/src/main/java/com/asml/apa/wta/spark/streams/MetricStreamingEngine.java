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

  private final KeyedStream<String, SparkBaseSupplierWrapperDto> resourceStream;
  private final KeyedStream<TaskKey, TaskMetricsRecord> taskStream;

  /**
   * Initializes the streams.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public MetricStreamingEngine() {
    resourceStream = new KeyedStream<>();
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
    resourceStream.addToStream(resourceKey, record);
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
}
