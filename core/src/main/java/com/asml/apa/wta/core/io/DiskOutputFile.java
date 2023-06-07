package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Disk {@link Path} implementation of the {@link OutputFile} abstraction.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
@RequiredArgsConstructor
public class DiskOutputFile implements OutputFile {

  private final Path file;

  @Override
  public OutputFile resolve(String path) {
    return new DiskOutputFile(file.resolve(path));
  }

  @Override
  public BufferedOutputStream open() throws IOException {
    return new BufferedOutputStream(Files.newOutputStream(file));
  }

  /**
   * Helper to delete files.
   *
   * @param path a {@link Path} pointing to the file to delete
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  private void deleteFile(Path path) {
    try {
      Files.delete(path);
    } catch (IOException e) {
      log.error("Could not delete file {}.", path);
    }
  }

  @Override
  public void clearDirectory() throws IOException {
    Files.createDirectories(file);
    try (Stream<Path> paths = Files.walk(file)) {
      paths.sorted(Comparator.reverseOrder()).forEach(this::deleteFile);
    }
  }

  @Override
  public String toString() {
    return file.toString();
  }
}
