package com.asml.apa.wta.core.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class BashUtilsIntegrationTest {
  @Test
  void runExecuteCommandSuccessfully() {
    BashUtils bashUtils = new BashUtils();
    CompletableFuture<String> actual = bashUtils.executeCommand("echo hello");
    assertEquals(actual.join(), "hello\n");
  }
}
