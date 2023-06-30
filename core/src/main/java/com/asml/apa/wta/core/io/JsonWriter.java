package com.asml.apa.wta.core.io;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedOutputStream;
import java.io.Flushable;
import java.io.IOException;

/**
 * Writes records to a JSON file.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class JsonWriter<T> implements AutoCloseable, Flushable {

  private final BufferedOutputStream outputStream;

  /**
   * Constructs a writer to write records as JSON.
   *
   * @param path the {@link OutputFile} to write to
   */
  public JsonWriter(OutputFile path) throws IOException {
    outputStream = path.open();
  }

  /**
   * Writes object as JSON.
   *
   * @param record the record to write
   * @throws IOException when something goes wrong when writing
   */
  public void write(T record) throws IOException {
    Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    outputStream.write(gson.toJson(record).getBytes());
  }

  /**
   * Closes the writer.
   *
   * @throws IOException when something goes wrong during I/O
   */
  @Override
  public void close() throws IOException {
    outputStream.close();
  }

  /**
   * Flushes the writer.
   *
   * @throws IOException when something goes wrong during I/O
   */
  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }
}
