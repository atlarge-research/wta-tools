package com.asml.apa.wta.hdfs.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.io.OutputFileFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

class OutputFileFactoryWithHdfsTest {

  @Test
  @EnabledOnOs({OS.WINDOWS})
  void createSimpleLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("C:/path/to/output");
    assertThat(file).isExactlyInstanceOf(HadoopOutputFile.class);
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void creatRelativeLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("path/to/output");
    assertThat(file).isExactlyInstanceOf(HadoopOutputFile.class);
  }
}
