package com.asml.apa.wta.core.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class ShellUtilsIntegrationTest {

  @Test
  @EnabledOnOs(OS.LINUX)
  void runExecuteCommandSuccessfully() {
    ShellUtils shellUtils = new ShellUtils();
    CompletableFuture<String> actual = shellUtils.executeCommand("echo hello", false);
    assertEquals(actual.join(), "hello\n");
  }
}
