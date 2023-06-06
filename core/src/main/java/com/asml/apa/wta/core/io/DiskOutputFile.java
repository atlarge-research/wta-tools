package com.asml.apa.wta.core.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@RequiredArgsConstructor
public class DiskOutputFile implements OutputFile {

  private final Path file;
  private BufferedWriter writer;

  public OutputFile resolve(String path) {
    return new DiskOutputFile(file.resolve(path), null);
  }

  public OutputFile open() throws IOException {
    writer = Files.newBufferedWriter(file);
    return this;
  }

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
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }
}
