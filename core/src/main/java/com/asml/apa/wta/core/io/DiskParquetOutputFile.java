package com.asml.apa.wta.core.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * Disk Parquet output file to wrap {@link Path}s for the {@link org.apache.parquet.hadoop.ParquetWriter}.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class DiskParquetOutputFile implements OutputFile {

  private final Path path;

  /**
   * Constructs a {@link DiskParquetOutputFile} from a {@link Path}.
   *
   * @param file the path to wrap for the writer
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public DiskParquetOutputFile(Path file) {
    path = file;
  }

  /**
   * Creates a {@link PositionOutputStream} for the wrapped {@link Path}.
   *
   * @param buffer buffer hint, should not exceed {@link Integer#MAX_VALUE}
   * @return the created {@link PositionOutputStream}
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public PositionOutputStream create(long buffer) throws IOException {
    log.debug("Create org.apache.parquet.io.PositionOutputStream with {} buffer.", buffer);

    return new PositionOutputStream() {

      private final BufferedOutputStream stream =
          new BufferedOutputStream(Files.newOutputStream(path), (int) buffer);
      private long pos = 0;

      /**
       * Get current position in the {@link BufferedOutputStream}.
       *
       * @return the current position
       * @author Atour Mousavi Gourabi
       * @since 1.0.0
       */
      @Override
      public long getPos() {
        log.debug("Position at {}.", pos);
        return pos;
      }

      /**
       * Writes a byte from {@code data}.
       *
       * @see BufferedOutputStream#write(int)
       *
       * @param data the {@code byte}
       * @throws IOException when something goes wrong during I/O
       * @author Atour Mousavi Gourabi
       * @since 1.0.0
       */
      @Override
      public void write(int data) throws IOException {
        log.debug("Write {}.", data);
        pos++;
        stream.write(data);
      }

      /**
       * Writes the bytes from {@code data}.
       *
       * @see BufferedOutputStream#write(byte[])
       *
       * @param data the data
       * @throws IOException when something goes wrong during I/O
       * @author Atour Mousavi Gourabi
       * @since 1.0.0
       */
      @Override
      public void write(byte[] data) throws IOException {
        log.info("write {}", data);
        pos += data.length;
        stream.write(data);
      }

      /**
       * Writes {@code len} bytes from {@code data} starting at {@code off}.
       *
       * @see BufferedOutputStream#write(byte[], int, int)
       *
       * @param data the data
       * @param off the start offset in the data
       * @param len the number of bytes to write
       * @throws IOException when something goes wrong during I/O
       * @author Atour Mousavi Gourabi
       * @since 1.0.0
       */
      @Override
      public void write(byte[] data, int off, int len) throws IOException {
        log.info("write {}", data);
        pos += len;
        stream.write(data, off, len);
      }

      /**
       * Flushes the {@link DiskParquetOutputFile}.
       *
       * @throws IOException when something goes wrong during I/O
       * @author Atour Mousavi Gourabi
       * @since 1.0.0
       */
      @Override
      public void flush() throws IOException {
        stream.flush();
      }

      /**
       * Closes the {@link DiskParquetOutputFile}.
       *
       * @throws IOException when something goes wrong during I/O
       * @author Atour Mousavi Gourabi
       * @since 1.0.0
       */
      @Override
      public void close() throws IOException {
        stream.close();
      }
    };
  }

  /**
   * Creates a {@link PositionOutputStream} for the wrapped {@link Path}.
   *
   * @see DiskParquetOutputFile#create(long)
   *
   * @param buffer buffer hint
   * @return the created {@link PositionOutputStream}
   * @throws IOException when something goes wrong during I/O
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public PositionOutputStream createOrOverwrite(long buffer) throws IOException {
    return create(buffer);
  }

  /**
   * Checks whether the output file supports block size.
   *
   * @return {@code true}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public boolean supportsBlockSize() {
    return true;
  }

  /**
   * Returns the default block size.
   *
   * @return {@code 512}, the default value for {@link BufferedOutputStream}
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  @Override
  public long defaultBlockSize() {
    return 512;
  }
}
