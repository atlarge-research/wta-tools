package com.asml.apa.wta.spark.streams;

import com.asml.apa.wta.core.streams.StreamRecord;

/**
 * Resource metrics record for the Apache Spark adapter.
 * To hold all the metrics we could get from the executor plugins to be moved to the driver.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class ResourceMetricsRecord implements StreamRecord<ResourceMetricsRecord> {

  private ResourceMetricsRecord next;

  /**
   * Sets the next record reference.
   *
   * @param next the record to add
   * @return the new record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public ResourceMetricsRecord setNext(ResourceMetricsRecord next) {
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
  public ResourceMetricsRecord getNext() {
    return next;
  }
}
