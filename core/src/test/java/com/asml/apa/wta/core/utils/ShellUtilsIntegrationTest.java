package com.asml.apa.wta.core.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class ShellUtilsIntegrationTest {
  @Test
  void runExecuteCommandSuccessfully() {
    ShellUtils shellUtils = new ShellUtils();
    CompletableFuture<String> actual = shellUtils.executeCommand("echo hello");
    assertEquals(actual.join(), "hello\n");
  }
}
