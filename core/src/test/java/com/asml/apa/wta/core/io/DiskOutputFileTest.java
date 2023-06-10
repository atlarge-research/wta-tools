package com.asml.apa.wta.core.io;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class DiskOutputFileTest {

  @Test
  void resolve() {
    OutputFile file = new DiskOutputFile(Path.of("folder"));
    assertThat(file.toString()).isEqualTo("folder");
    file = file.resolve("subfolder");
    assertThat(file.toString()).isEqualTo("folder\\subfolder");
  }
}
