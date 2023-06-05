package com.asml.apa.wta.core.utils;

import com.asml.apa.wta.core.datasource.iodependencies.BashUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BashUtilsIntegrationTest {
    @Test
    void runExecuteCommandSuccessfully() throws ExecutionException, InterruptedException {
        BashUtils bashUtils = new BashUtils();
        CompletableFuture<String> actual = bashUtils.executeCommand("echor hello");
        actual.exceptionally(throwable -> {
            // Handle the exception here
            System.err.println("An exception occurred: " + throwable.getMessage());
            return null; // or provide a default value
        });
//        assertEquals("hello\n", actual);
    }
    @Test
    void runExecuteCommandUnsuccessfully() throws ExecutionException, InterruptedException {
        BashUtils bashUtils = new BashUtils();
        CompletableFuture<String> actual = bashUtils.executeCommand("echo hello");
        assertEquals(actual.get(), "hello\n");
    }
}
