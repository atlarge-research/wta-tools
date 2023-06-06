package com.asml.apa.wta.hadoop.io;

import com.asml.apa.wta.core.io.OutputFile;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
@RequiredArgsConstructor
public class HadoopOutputFile implements OutputFile {

  private final Path file;
  private BufferedWriter writer;
  private FileSystem fs;

  @Override
  public OutputFile resolve(String path) {
    return new HadoopOutputFile(new Path(file, path), null, null);
  }

  @Override
  public OutputFile open() throws IOException {
    fs = FileSystem.get(new Configuration());
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(file)));
    return this;
  }

  @Override
  public void clearDirectory() throws IOException {
    fs.delete(file, true);
    fs.mkdirs(file);
  }

  @Override
  public Appendable append(CharSequence csq) throws IOException {
    return writer.append(csq);
  }

  @Override
  public Appendable append(CharSequence csq, int start, int end) throws IOException {
    return writer.append(csq, start, end);
  }

  @Override
  public Appendable append(char c) throws IOException {
    return writer.append(c);
  }

  @Override
  public void close() throws Exception {
    writer.close();
    fs.close();
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }
}
