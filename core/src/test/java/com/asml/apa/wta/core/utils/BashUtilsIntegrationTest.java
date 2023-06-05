package com.asml.apa.wta.core.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class BashUtilsIntegrationTest {
  @Test
  void runExecuteCommandSuccessfully() {
    BashUtils bashUtils = new BashUtils();
    CompletableFuture<String> failedCommand = bashUtils.executeCommand("invalid_command");
    ExecutionException exception = assertThrows(ExecutionException.class, () -> {
      failedCommand.get(); // Wait for the result and expect an exception
    });
    Throwable cause = exception.getCause();
    assertTrue(cause instanceof BashUtils.BashCommandExecutionException);
    assertEquals("Bash command execution failed with exit code: 127", cause.getMessage());
  }

  @Test
  void runExecuteCommandUnsuccessfully() throws ExecutionException, InterruptedException {
    BashUtils bashUtils = new BashUtils();
    CompletableFuture<String> actual = bashUtils.executeCommand("echo hello");
    assertEquals(actual.get(), "hello\n");
  }
}
