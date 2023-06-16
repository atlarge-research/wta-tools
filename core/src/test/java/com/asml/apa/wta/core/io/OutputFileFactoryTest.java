package com.asml.apa.wta.core.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.InvalidPathException;
import org.junit.jupiter.api.Test;

class OutputFileFactoryTest {

  @Test
  void createDiskLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("C:/path/to/output");
    assertThat(file).isExactlyInstanceOf(DiskOutputFile.class);
  }

  @Test
  void createHdfsLocationWithoutHdfs() {
    OutputFileFactory factory = new OutputFileFactory();
    assertThatThrownBy(() -> factory.create("hdfs://path/to/output")).isInstanceOf(InvalidPathException.class);
  }
}
