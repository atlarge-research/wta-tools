package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.exceptions.FailedToSerializeStreamException;
import com.asml.apa.wta.core.streams.KeyedStream;

/**
 * Facade that maintains the resource and task streams.
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
   * Adds resource metrics to the resource stream.
   *
   * @param resource the {@link com.asml.apa.wta.spark.streams.ResourceKey} of the resource
   * @param record the {@link ResourceMetricsRecord} containing the metrics
   * @throws FailedToSerializeStreamException when some error occurred during routine serialization of parts of
   *                                          the {@link com.asml.apa.wta.core.streams.KeyedStream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToResourceStream(ResourceKey resource, ResourceMetricsRecord record)
      throws FailedToSerializeStreamException {
    resourceStream.addToStream(resource, record);
  }

  /**
   * Adds task metrics to the task stream.
   *
   * @param task the {@link com.asml.apa.wta.spark.streams.TaskKey} of the task
   * @param record the {@link TaskMetricsRecord} containing the metrics
   * @throws FailedToSerializeStreamException when some error occurred during routine serialization of parts of
   *                                          the {@link com.asml.apa.wta.core.streams.KeyedStream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void addToTaskStream(TaskKey task, TaskMetricsRecord record) throws FailedToSerializeStreamException {
    taskStream.addToStream(task, record);
  }
}
