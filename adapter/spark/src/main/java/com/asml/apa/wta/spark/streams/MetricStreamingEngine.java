package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.streams.KeyedStream;

/**
 * Maintains the resource and task streams.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class MetricStreamingEngine {

  private final KeyedStream<ResourceKey, ResourceMetricsRecord> resourceStream;
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
   * Adds a resource metrics to the resource stream.
   *
   * @param resource the {@link com.asml.apa.wta.spark.streams.ResourceKey} of the resource
   * @param record the {@link com.asml.apa.wta.spark.streams.ResourceMetricsRecord} containing the metrics
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToResourceStream(ResourceKey resource, ResourceMetricsRecord record) {
    resourceStream.addToStream(resource, record);
  }

  /**
   * Adds a resource metrics to the resource stream.
   *
   * @param task the {@link com.asml.apa.wta.spark.streams.TaskKey} of the task
   * @param record the {@link com.asml.apa.wta.spark.streams.TaskMetricsRecord} containing the metrics
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToTaskStream(TaskKey task, TaskMetricsRecord record) {
    taskStream.addToStream(task, record);
  }
}
