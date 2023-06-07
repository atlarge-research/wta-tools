package com.asml.apa.wta.core.io;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedOutputStream;
import java.io.Flushable;
import java.io.IOException;

/**
 * Interface to write files to JSON.
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
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public JsonWriter(OutputFile path) throws IOException {
    outputStream = path.open();
  }

  /**
   * Writes object as JSON.
   *
   * @param record the record to write as JSON
   * @throws IOException when something goes wrong during writing
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public void write(T record) throws Exception {
    Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    outputStream.write(gson.toJson(record).getBytes());
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
