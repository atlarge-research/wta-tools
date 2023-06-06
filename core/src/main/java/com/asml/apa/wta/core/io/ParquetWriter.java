package com.asml.apa.wta.core.io;

/**
 * Interface to write files to Parquet.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public interface ParquetWriter<T> {

  /**
   * Writes the record.
   * Provides no guarantee that the file is directly flushed.
   *
   * @param record the record to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  void write(T record);

  /**
   * Flushes the written records.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  void flush();
}
