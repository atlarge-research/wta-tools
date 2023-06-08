package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

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
    return new PositionOutputStream() {

      BufferedOutputStream stream = new BufferedOutputStream(Files.newOutputStream(path), (int) l);
      long pos = 0;

      @Override
      public long getPos() throws IOException {
        return pos;
      }

      @Override
      public void write(int b) throws IOException {
        ++pos;
        stream.write(b);
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
