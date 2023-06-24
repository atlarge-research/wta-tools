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
   * @param path a {@link String} representing the path to resolve
   * @return the resolved location
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  OutputFile resolve(String path);

  /**
   * Sets the path of the output file.
   *
   * @param path a {@link String} representation of the location to point to
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  void setPath(String path) throws IOException;

  /**
   * Signals whether this implementation can output to the specified location.
   *
   * @param path a {@link String} representation of the location to point to
   * @return a {@code boolean} indicating whether the implementation can handle the given location
   */
  boolean acceptsLocation(String path);

  /**
   * Open a writer resource for the {@link OutputFile}.
   *
   * @return an opened {@link OutputFile} writer
   * @throws IOException when no writer can be opened for the location of this {@link OutputFile}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  BufferedOutputStream open() throws IOException;

  /**
   * Clear the current directory if this {@link OutputFile} points to a folder.
   * If the location this points to does not exist yet, the directory is created.
   *
   * @return the {@link OutputFile} pointing to the cleared directory
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  OutputFile clearDirectory() throws IOException;

  /**
   * Wraps this {@link OutputFile} into a Parquet {@link org.apache.parquet.io.OutputFile}.
   *
   * @return the wrapped disk path as a Parquet {@link org.apache.parquet.io.OutputFile}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  org.apache.parquet.io.OutputFile wrap() throws IOException;
}
