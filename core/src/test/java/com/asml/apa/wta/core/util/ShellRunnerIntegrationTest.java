package com.asml.apa.wta.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

class ShellRunnerIntegrationTest {

  @Test
  @EnabledOnOs(OS.LINUX)
  void runExecuteCommandSuccessfully() {
    ShellRunner shellRunner = new ShellRunner();
    CompletableFuture<String> actual = shellRunner.executeCommand("echo hello", false);
    assertEquals(actual.join(), "hello\n");
  }

  @Test
  @EnabledOnOs(OS.LINUX)
  void executeCommandWithErrorCode() {
    ShellRunner shellRunner = new ShellRunner();
    CompletableFuture<String> actual = shellRunner.executeCommand("echooo helloooo", false);
    assertNull(actual.join());
  }
}
