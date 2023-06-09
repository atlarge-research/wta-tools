package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

@Slf4j
public class DiskParquetOutputFile implements OutputFile {

  private final Path path;

  public DiskParquetOutputFile(Path p) {
    path = p;
  }

  /**
   * @param l
   * @return
   * @throws IOException
   */
  @Override
  public PositionOutputStream create(long l) throws IOException {
    log.info("create {}", l);

    return new PositionOutputStream() {

      BufferedOutputStream stream = new BufferedOutputStream(Files.newOutputStream(path), (int) l);
      long pos = 0;

      @Override
      public long getPos() throws IOException {
        log.info("pos {}", pos);

        return pos;
      }

      @Override
      public void write(int b) throws IOException {
        log.info("write {}", b);
        ++pos;
        stream.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
        pos += b.length;
        stream.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        pos += b.length;
        stream.write(b, off, len);
      }

      @Override
      public void flush() throws IOException {
        stream.flush();
      }

      @Override
      public void close() throws IOException {
        stream.close();
      }
    };
  }

  /**
   * @param l
   * @return
   * @throws IOException
   */
  @Override
  public PositionOutputStream createOrOverwrite(long l) throws IOException {
    log.info("create or overwrite {}", l);

    return create(l);
  }

  /**
   * @return
   */
  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  /**
   * @return
   */
  @Override
  public long defaultBlockSize() {
    return 0;
  }
}
