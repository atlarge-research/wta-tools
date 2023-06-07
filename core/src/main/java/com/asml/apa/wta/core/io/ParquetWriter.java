package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.Flushable;
import java.io.IOException;

/**
 * Interface to write files to Parquet.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class ParquetWriter<T> implements AutoCloseable, Flushable {

  private final BufferedOutputStream outputStream;

  /**
   * Constructs a writer to write records as Parquet.
   *
   * @param path the {@link OutputFile} to write to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public ParquetWriter(OutputFile path) throws IOException {
    outputStream = path.open();
  }

  /**
   * Writes the record.
   * Provides no guarantee that the file is directly flushed.
   *
   * @param record the record to write
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write(T record) throws Exception {
    outputStream.write(1);
    // todo
  }

  @Override
  public void close() throws Exception {
    outputStream.close();
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }
}
