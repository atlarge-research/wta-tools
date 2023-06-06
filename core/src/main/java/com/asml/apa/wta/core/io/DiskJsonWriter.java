package com.asml.apa.wta.core.io;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes objects to disk in JSON.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public class DiskJsonWriter<T> implements JsonWriter<T> {

  private final Path outputPath;

  /**
   * Constructs a JSON writer to write to disk.
   *
   * @param path the {@link Path} to write to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public DiskJsonWriter(Path path) {
    outputPath = path;
  }

  /**
   * Writes a record to disk in JSON.
   *
   * @param record the record to write to file
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public void write(T record) throws IOException {
    Gson gson = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create();
    try (BufferedWriter fw = Files.newBufferedWriter(outputPath)) {
      gson.toJson(record, fw);
    }
  }
}
