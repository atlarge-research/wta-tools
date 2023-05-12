package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.streams.StreamRecord;

/**
 * Task metrics record for the Apache Spark adapter.
 * To hold all the task metrics we could get from the executor plugins to be moved to the driver.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class TaskMetricsRecord implements StreamRecord<TaskMetricsRecord> {

  private TaskMetricsRecord next;

  /**
   * Sets the next record reference.
   *
   * @param next the record to add
   * @return the new record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public TaskMetricsRecord setNext(TaskMetricsRecord next) {
    this.next = next;
    return next;
  }

  /**
   * Gets the next record.
   *
   * @return the next record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public TaskMetricsRecord getNext() {
    return next;
  }
}
