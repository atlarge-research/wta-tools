package com.asml.apa.wta.hadoop.io;

import com.asml.apa.wta.core.io.OutputFile;
import java.io.BufferedOutputStream;
import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Hadoop {@link Path} implementation of the {@link OutputFile} abstraction.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
@RequiredArgsConstructor
public class HadoopOutputFile implements OutputFile {

  private final Path file;
  private final Configuration conf;
  private final FileSystem fs;

  /**
   * Constructs a HadoopOutputFile.
   *
   * @param path the {@link Path} to construct a {@link HadoopOutputFile} for
   * @param configuration the {@link Configuration} to use
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public HadoopOutputFile(Path path, Configuration configuration) throws IOException {
    file = path;
    conf = configuration;
    fs = path.getFileSystem(new Configuration());
  }

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
    return new HadoopOutputFile(new Path(file, path), conf, fs);
  }

  /**
   * Open a writer resource for the {@link OutputFile}.
   *
   * @return an opened {@link OutputFile} writer
   * @throws IOException when no writer can be opened for the location of this {@link OutputFile}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public BufferedOutputStream open() throws IOException {
    return new BufferedOutputStream(fs.create(file));
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
    fs.delete(file, true);
    fs.mkdirs(file);
  }

  /**
   * Wraps this {@link HadoopOutputFile} into a Parquet {@link org.apache.parquet.io.OutputFile}.
   *
   * @return the wrapped Hadoop path as a Parquet {@link org.apache.parquet.io.OutputFile}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public org.apache.parquet.io.OutputFile wrap() throws IOException {
    return org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(file, conf);
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
