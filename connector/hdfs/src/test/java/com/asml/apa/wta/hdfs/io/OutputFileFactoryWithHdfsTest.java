package com.asml.apa.wta.hdfs.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.io.OutputFile;
import com.asml.apa.wta.core.io.OutputFileFactory;
import org.junit.jupiter.api.Test;

class OutputFileFactoryWithHdfsTest {

  @Test
  void createSimpleLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("C:/path/to/output");
    assertThat(file).isExactlyInstanceOf(HadoopOutputFile.class);
  }

  @Test
  void creatRelativeLocation() {
    OutputFileFactory factory = new OutputFileFactory();
    OutputFile file = factory.create("path/to/output");
    assertThat(file).isExactlyInstanceOf(HadoopOutputFile.class);
  }
}
