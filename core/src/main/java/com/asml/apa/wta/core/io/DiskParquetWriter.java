package com.asml.apa.wta.core.io;

import java.nio.file.Path;

/**
 * Writes records to disk in Parquet.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class DiskParquetWriter<T> implements ParquetWriter<T> {

  private final Path outputPath;

  /**
   * Constructs a Parquet writer to write to disk.
   *
   * @param path the {@link Path} to write to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public DiskParquetWriter(Path path) {
    outputPath = path;
  }

  /**
   * Writes the record.
   * Provides no guarantees that the file is directly flushed.
   *
   * @param record the record to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public void write(T record) {}

  /**
   * Flushes the written records to disk.
   *
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public void flush() {}
}
