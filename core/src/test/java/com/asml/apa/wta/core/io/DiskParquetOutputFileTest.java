package com.asml.apa.wta.core.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.Test;

public class DiskParquetOutputFileTest {
  @Test
  void testGetPos() throws IOException {
    DiskParquetOutputFile diskParquetOutputFile = new DiskParquetOutputFile(Path.of("test.parquet"));
    PositionOutputStream sut = diskParquetOutputFile.createOrOverwrite(1);
    sut.write(42);
    assertEquals(1, sut.getPos());
    new File("test.parquet").delete();
  }

  @Test
  public void testGetPosAndWrite() throws IOException {
    // Create a temporary file for testing
    Path tempFile = Files.createTempFile("test", ".parquet");

    // Instantiate DiskParquetOutputFile
    DiskParquetOutputFile outputFile = new DiskParquetOutputFile(tempFile);

    // Create PositionOutputStream
    PositionOutputStream outputStream = outputFile.createOrOverwrite(1024);

    // Verify initial position
    assertEquals(0, outputStream.getPos());

    // Write some data and verify the updated position
    outputStream.write(42);
    assertEquals(1, outputStream.getPos());

    // Write byte array and verify the updated position
    byte[] data = {1, 2, 3, 4};
    outputStream.write(data);
    assertEquals(5, outputStream.getPos());

    // Write a portion of the byte array and verify the updated position
    outputStream.write(data, 1, 2);
    assertEquals(7, outputStream.getPos());

    assertTrue(outputFile.supportsBlockSize());
    assertEquals(512, outputFile.defaultBlockSize());

    // Close the output stream
    outputStream.close();

    // Perform any necessary cleanup
    Files.delete(tempFile);
  }
}
