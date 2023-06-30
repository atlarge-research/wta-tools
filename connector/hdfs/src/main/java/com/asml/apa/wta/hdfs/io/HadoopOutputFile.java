package com.asml.apa.wta.hdfs.io;

import com.asml.apa.wta.core.io.OutputFile;
import java.io.BufferedOutputStream;
import java.io.IOException;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class HadoopOutputFile implements OutputFile {

  private Path outputFile;

  private final Configuration conf;

  private FileSystem fs;

  /**
   * Default constructor for Java SPI.
   */
  public HadoopOutputFile() {
    conf = new Configuration();
  }

  /**
   * Constructs a HadoopOutputFile.
   *
   * @param path a {@link String} representation of the {@link Path} to construct a {@link HadoopOutputFile} for
   * @throws IOException when something goes wrong during I/O
   */
  public HadoopOutputFile(String path) throws IOException {
    outputFile = new Path(path);
    conf = new Configuration();
    fs = outputFile.getFileSystem(conf);
  }

  /**
   * Sets the path of the HDFS output file.
   *
   * @param path a {@link String} representation of the {@link Path} to point to
   * @throws IOException when something goes wrong during I/O
   */
  public void setPath(String path) throws IOException {
    outputFile = new Path(path);
    fs = outputFile.getFileSystem(conf);
  }

  /**
   * Signals whether this implementation can output to the specified location.
   *
   * @param path a {@link String} representation of the location to point to
   * @return a {@code boolean} indicating whether the implementation can handle the given location
   */
  @Override
  public boolean acceptsLocation(String path) {
    return true;
  }

  /**
   * Resolves a path in the current location.
   *
   * @param path a {@link String} representing the path to resolve
   * @return the resolved location
   */
  @Override
  public OutputFile resolve(String path) {
    return new HadoopOutputFile(new Path(outputFile, path), conf, fs);
  }

  /**
   * Open a writer resource for the {@link OutputFile}.
   *
   * @return an opened {@link OutputFile} writer
   * @throws IOException when no writer can be opened for the location of this {@link OutputFile}
   */
  @Override
  public BufferedOutputStream open() throws IOException {
    return new BufferedOutputStream(fs.create(outputFile));
  }

  /**
   * If the location this points to does not exist yet, the directory is created.
   *
   * @return the {@link OutputFile} pointing to the cleared directory
   * @throws IOException when something goes wrong during I/O
   */
  @Override
  public OutputFile clearDirectories() throws IOException {
    fs.mkdirs(outputFile);
    fs.delete(outputFile, true);
    return this;
  }

  /**
   * Wraps this {@link HadoopOutputFile} into a Parquet {@link org.apache.parquet.io.OutputFile}.
   *
   * @return the wrapped Hadoop path as a Parquet {@link org.apache.parquet.io.OutputFile}
   */
  @Override
  public org.apache.parquet.io.OutputFile wrap() throws IOException {
    return org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(outputFile, conf);
  }

  /**
   * Converts the object to a {@link String} for printing.
   *
   * @return a {@link String} representing the path this object points to
   */
  @Override
  public String toString() {
    return outputFile.toString();
  }
}
