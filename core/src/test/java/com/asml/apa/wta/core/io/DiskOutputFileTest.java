package com.asml.apa.wta.core.io;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

class DiskOutputFileTest {

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void resolveWindows() {
    OutputFile file = new DiskOutputFile(Path.of("folder"));
    assertThat(file.toString()).isEqualTo("folder");
    file = file.resolve("subfolder");
    assertThat(file.toString()).isEqualTo("folder\\subfolder");
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void resolveLinux() {
    OutputFile file = new DiskOutputFile(Path.of("folder"));
    assertThat(file.toString()).isEqualTo("folder");
    file = file.resolve("subfolder");
    assertThat(file.toString()).isEqualTo("folder/subfolder");
  }

  @TempDir
  public Path tempDirectory;

  @Test
  public void testClearDirectory() throws IOException {
    // Create some files and subdirectories inside the temporary directory
    createFile(tempDirectory, "file1.txt");
    createFile(tempDirectory, "file2.txt");
    createSubdirectory(tempDirectory, "subdir1");
    createSubdirectory(tempDirectory, "subdir2");

    DiskOutputFile outputFile = new DiskOutputFile(tempDirectory);

    outputFile.clearDirectories();
    assertEquals(0, Files.list(tempDirectory).count(), "Directory is not empty");
  }

  private void createFile(Path parentDirectory, String fileName) throws IOException {
    File file = parentDirectory.resolve(fileName).toFile();
    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
    out.close();
  }

  private void createSubdirectory(Path parentDirectory, String subdirectoryName) throws IOException {
    Files.createDirectory(parentDirectory.resolve(subdirectoryName));
  }
}
