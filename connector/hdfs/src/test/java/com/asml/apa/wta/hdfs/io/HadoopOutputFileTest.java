package com.asml.apa.wta.hdfs.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.io.OutputFileFactory;
import java.io.BufferedOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class HadoopOutputFileTest {

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void resolveSimpleLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("C:/path/to/output");
    file = file.resolve("subdirectory");
    assertThat(file).isNotNull();
    assertThat(file).isExactlyInstanceOf(HadoopOutputFile.class);
  }

  @Test
  void resolveRelativeLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("path/to/output");
    file = file.resolve("subdirectory");
    assertThat(file).isNotNull();
    assertThat(file).isExactlyInstanceOf(HadoopOutputFile.class);
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void clearRelativeLocation() throws IOException {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("path/to/output");
    file = file.clearDirectories();
    assertThat(file).isNotNull();
    assertThat(file).isExactlyInstanceOf(HadoopOutputFile.class);
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void openRelativeLocation() throws IOException {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("path/to/output");
    BufferedOutputStream out = file.open();
    assertThat(out).isNotNull();
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void wrapRelativeLocation() throws IOException {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("path/to/output");
    org.apache.parquet.io.OutputFile out = file.wrap();
    assertThat(out).isNotNull();
    assertThat(out).isExactlyInstanceOf(org.apache.parquet.hadoop.util.HadoopOutputFile.class);
  }

  @Test
  void printRelativeLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("path/to/output");
    String location = file.toString();
    assertThat(location).isNotNull();
    assertThat(location).isEqualTo("path/to/output");
  }

  @Test
  @EnabledOnOs(OS.WINDOWS)
  void printSimpleLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("C:/path/to/output");
    String location = file.toString();
    assertThat(location).isNotNull();
    assertThat(location).isEqualTo("C:/path/to/output");
  }
}
