package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.streams.StreamRecord;

/**
 * Metrics record for the Apache Spark adapter.
 * To hold all the metrics we could get from the executor plugins to be moved to the driver.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class MetricsRecord implements StreamRecord<MetricsRecord> {
  private MetricsRecord next;

  /**
   * Sets the following record.
   *
   * @param next the record to add
   * @return the new record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public MetricsRecord setNext(MetricsRecord next) {
    this.next = next;
    return next;
  }

  /**
   * Gets the following record.
   *
   * @return the next record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public MetricsRecord getNext() {
    return next;
  }
}
