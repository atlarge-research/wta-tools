package com.asml.apa.wta.core.io;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

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
  @EnabledOnOs(OS.LINUX)
  void resolveLinux() {
    OutputFile file = new DiskOutputFile(Path.of("folder"));
    assertThat(file.toString()).isEqualTo("folder");
    file = file.resolve("subfolder");
    assertThat(file.toString()).isEqualTo("folder/subfolder");
  }
}
