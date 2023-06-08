package com.asml.apa.wta.hadoop.io;

import com.asml.apa.wta.core.io.OutputFile;
import java.io.BufferedOutputStream;
import java.io.IOException;
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
public class HadoopOutputFile implements OutputFile {

  private final Path file;
  private final Configuration conf;
  private final FileSystem fs;

  public HadoopOutputFile(Path path, Configuration configuration) {
    file = path;
    conf = configuration;
    try {
      fs = path.getFileSystem(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  public HadoopOutputFile(Path path, Configuration configuration, FileSystem fileSystem) {
    file = path;
    conf = configuration;
    fs = fileSystem;
  }

  @Override
  public OutputFile resolve(String path) {
    return new HadoopOutputFile(new Path(file, path), conf, fs);
  }

  @Override
  public BufferedOutputStream open() throws IOException {
    return new BufferedOutputStream(fs.create(file));
  }

  @Override
  public void clearDirectory() throws IOException {
    fs.delete(file, true);
    fs.mkdirs(file);
  }

  /**
   * @return
   */
  @Override
  public org.apache.parquet.io.OutputFile wrap() throws IOException {
    return org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(file, conf);
  }

  @Override
  public String toString() {
    return file.toString();
  }
}
