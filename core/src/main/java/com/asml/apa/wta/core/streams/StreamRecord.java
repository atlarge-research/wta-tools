package com.asml.apa.wta.core.streams;

/**
 * Stream record.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public interface StreamRecord<E extends StreamRecord<E>> {

  /**
   * Sets the following record.
   *
   * @param next the record to add
   * @return the new record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  E setNext(E next);

  /**
   * Gets the following record.
   *
   * @return the next record
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  E getNext();
}
