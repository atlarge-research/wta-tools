package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.IOException;

/**
 * Output file abstraction over the output location of generated files.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
public interface OutputFile {

  /**
   * Resolves a path in the current location.
   *
   * @param path          {@link String} representing the path to resolve
   * @return              resolved location
   * @since 1.0.0
   */
  OutputFile resolve(String path);

  /**
   * Sets the path of the output file.
   *
   * @param path          {@link String} representation of the location to point to
   * @throws IOException  when something goes wrong during I/O
   * @since 1.0.0
   */
  void setPath(String path) throws IOException;

  /**
   * Signals whether this implementation can output to the specified location.
   *
   * @param path          {@link String} representation of the location to point to
   * @return              {@code boolean} indicating whether the implementation can handle the given location
   * @since 1.0.0
   */
  boolean acceptsLocation(String path);

  /**
   * Open a writer resource for the {@link OutputFile}.
   *
   * @return              opened {@link OutputFile} writer
   * @throws IOException  when no writer can be opened for the location of this {@link OutputFile}
   * @since 1.0.0
   */
  BufferedOutputStream open() throws IOException;

  /**
   * If the location this points to does not exist yet, the directory is created.
   *
   * @return              {@link OutputFile} pointing to the cleared directory
   * @throws IOException  when something goes wrong during I/O
   * @since 1.0.0
   */
  OutputFile clearDirectories() throws IOException;

  /**
   * Wraps this {@link OutputFile} into a Parquet {@link org.apache.parquet.io.OutputFile}.
   *
   * @return              wrapped disk path as a Parquet {@link org.apache.parquet.io.OutputFile}
   * @throws IOException  when something goes wrong during I/O
   * @since 1.0.0
   */
  org.apache.parquet.io.OutputFile wrap() throws IOException;
}
