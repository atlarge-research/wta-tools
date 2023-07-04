package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Disk {@link Path} implementation of the {@link OutputFile} abstraction.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
@AllArgsConstructor
public class DiskOutputFile implements OutputFile {

  private Path outputFile;

  /**
   * Resolves a path in the current location.
   *
   * @param path          {@link String} representing the path to resolve
   * @return              resolved location
   * @since 1.0.0
   */
  @Override
  public OutputFile resolve(String path) {
    Path resolved = outputFile.resolve(path);
    log.debug("Resolves {} and {} to {}.", this, path, resolved);
    return new DiskOutputFile(outputFile.resolve(path));
  }

  /**
   * Signals whether this implementation can output to the specified location.
   *
   * @param path          {@link String} representation of the location to point to
   * @return              {@code boolean} indicating whether the implementation can handle the given location
   * @since 1.0.0
   */
  @Override
  public boolean acceptsLocation(String path) {
    return true;
  }

  /**
   * Sets the path of the disk output file.
   *
   * @param path          {@link String} representation of the {@link Path} to point to
   * @since 1.0.0
   */
  public void setPath(String path) {
    outputFile = Path.of(path);
  }

  /**
   * Open a writer resource for the {@link OutputFile}. Overwrites existing files when necessary.
   *
   * @return              opened {@link OutputFile} writer
   * @throws IOException  when no writer can be opened for the location of this {@link OutputFile}
   * @since 1.0.0
   */
  @Override
  public BufferedOutputStream open() throws IOException {
    log.debug("Open stream at {}.", outputFile);
    return new BufferedOutputStream(
        Files.newOutputStream(outputFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
  }

  /**
   * If the location this points to does not exist yet, the directory is created.
   *
   * @return              {@link OutputFile} pointing to the cleared directory
   * @throws IOException  when something goes wrong during I/O
   * @since 1.0.0
   */
  @Override
  public OutputFile clearDirectories() throws IOException {
    Files.createDirectories(outputFile);
    try (Stream<Path> stream = Files.walk(outputFile)) {
      stream.sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .filter(file -> !outputFile.toFile().equals(file))
          .forEach(File::delete);
    }
    log.debug("Created and cleared the directory at {}.", outputFile.toString());
    return this;
  }

  /**
   * Wraps this {@link DiskOutputFile} into a Parquet {@link org.apache.parquet.io.OutputFile}.
   *
   * @return              wrapped disk path as a Parquet {@link org.apache.parquet.io.OutputFile}
   * @since 1.0.0
   */
  @Override
  public org.apache.parquet.io.OutputFile wrap() {
    log.debug("Wrapping {} with org.apache.parquet.io.OutputFile.", this);
    return new DiskParquetOutputFile(outputFile);
  }

  /**
   * Converts the object to a {@link String} for printing.
   *
   * @return              {@link String} representing the path this object points to
   * @since 1.0.0
   */
  @Override
  public String toString() {
    return outputFile.toString();
  }
}
