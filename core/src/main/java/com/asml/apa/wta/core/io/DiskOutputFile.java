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

  /**
   * Resolves a path in the current location.
   *
   * @param path a {@link String} representing the path to resolve
   * @return the resolved location
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public OutputFile resolve(String path) {
    Path resolved = file.resolve(path);
    log.debug("Resolves {} and {} to {}.", this, path, resolved);
    return new DiskOutputFile(file.resolve(path));
  }

  /**
   * Open a writer resource for the {@link OutputFile}.
   *
   * @return an opened {@link OutputFile} writer.
   * @throws IOException when no writer can be opened for the location of this {@link OutputFile}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public BufferedOutputStream open() throws IOException {
    log.debug("Open stream at {}.", file);
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
      log.debug("Deleted file at {}.", path);
    } catch (IOException e) {
      log.error("Could not delete file at {}.", path);
    }
  }

  /**
   * Clear the current directory if this {@link OutputFile} points to a folder.
   * If the location this points to does not exist yet, the directory is created.
   *
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public void clearDirectory() throws IOException {
    Files.createDirectories(file);
    try (Stream<Path> paths = Files.walk(file)) {
      paths.sorted(Comparator.reverseOrder()).forEach(this::deleteFile);
    }
    log.debug("Cleared the directory at {}.", file);
  }

  /**
   * Wraps this {@link DiskOutputFile} into a Parquet {@link org.apache.parquet.io.OutputFile}.
   *
   * @return the wrapped disk path as a Parquet {@link org.apache.parquet.io.OutputFile}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public org.apache.parquet.io.OutputFile wrap() {
    log.debug("Wrapping {} with org.apache.parquet.io.OutputFile.", this);
    return new DiskParquetOutputFile(file);
  }

  /**
   * Converts the object to a {@link String} for printing.
   *
   * @return a {@link String} representing the path this object points to
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public String toString() {
    return file.toString();
  }
}
