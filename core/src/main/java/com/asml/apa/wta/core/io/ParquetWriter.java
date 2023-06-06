package com.asml.apa.wta.core.io;

import java.io.Flushable;
import java.io.IOException;

/**
 * Interface to write files to Parquet.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class ParquetWriter<T> implements AutoCloseable, Flushable {

  private final OutputFile outputFile;

  /**
   * Constructs a writer to write records as Parquet.
   *
   * @param path the {@link OutputFile} to write to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public ParquetWriter(OutputFile path) {
    outputFile = path;
  }

  /**
   * Writes the record.
   * Provides no guarantee that the file is directly flushed.
   *
   * @param record the record to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write(T record) throws Exception {}

  @Override
  public void close() throws Exception {
    outputFile.close();
  }

  @Override
  public void flush() throws IOException {
    outputFile.flush();
  }
}
