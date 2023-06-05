package com.asml.apa.wta.core.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BashUtilsIntegrationTest {
    @Test
    void runExecuteCommandSuccessfully() {
        BashUtils bashUtils = new BashUtils();
        CompletableFuture<String> actual = bashUtils.executeCommand("echor hello");
        actual.exceptionally(throwable -> {
            System.err.println("An exception occurred: " + throwable.getMessage());
            return null;
        });
    }
    @Test
    void runExecuteCommandUnsuccessfully() throws ExecutionException, InterruptedException {
        BashUtils bashUtils = new BashUtils();
        CompletableFuture<String> actual = bashUtils.executeCommand("echo hello");
        assertEquals(actual.get(), "hello\n");
    }
}
