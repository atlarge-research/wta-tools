package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

/**
 * Writes records to a Parquet file.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class ParquetWriter<T> implements AutoCloseable, Flushable {

  private final BufferedOutputStream outputStream;

  private final String MAGIC_STR = "PAR1";
  private final byte[] MAGIC = MAGIC_STR.getBytes(StandardCharsets.US_ASCII);

  private State state;

  private enum State {
    STARTED,
    NOT_STARTED,
    BLOCK,
    COLUMN,
    ENDED
  }

  /**
   * Constructs a writer to write records to Parquet.
   *
   * @param path the {@link OutputFile} to write to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public ParquetWriter(OutputFile path) throws IOException {
    outputStream = path.open();
    log.debug("Started writing a Parquet file at {}.", path);
    state = State.STARTED;
    outputStream.write(MAGIC);
  }

  /**
   * Writes the record.
   * Provides no guarantee that the file is directly flushed.
   *
   * @param record the record to write
   * @throws IOException when something goes wrong when writing
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write(T record) throws IOException {
    outputStream.write(1);
    // todo
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }
}
