package com.asml.apa.wta.core.io;

import java.io.IOException;

/**
 * Interface to write files to JSON.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public interface JsonWriter<T> {

  /**
   * Writes object as JSON.
   *
   * @param record the record to write as JSON
   * @throws IOException when something goes wrong during writing
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  void write(T record) throws IOException;
}
