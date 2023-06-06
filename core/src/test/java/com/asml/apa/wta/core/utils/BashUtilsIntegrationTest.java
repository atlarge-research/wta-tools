package com.asml.apa.wta.core.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class BashUtilsIntegrationTest {
  @Test
  void runExecuteCommandSuccessfully() throws ExecutionException, InterruptedException {
    BashUtils bashUtils = new BashUtils();
    CompletableFuture<String> actual = bashUtils.executeCommand("echo hello");
    assertEquals(actual.get(), "hello\n");
  }
}
