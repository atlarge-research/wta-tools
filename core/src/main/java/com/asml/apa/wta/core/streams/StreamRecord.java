package com.asml.apa.wta.core.streams;

/**
 * Stream record.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public interface StreamRecord {

  /**
   * Sets the following record.
   *
   * @param next the record to add
   * @param <V> the concrete implementation type of StreamRecord
   * @return the new record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  <V extends StreamRecord> V setNext(V next);

  /**
   * Gets the following record.
   *
   * @param <V> the concrete implementation type of StreamRecord
   * @return the next record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  <V extends StreamRecord> V getNext();
}
