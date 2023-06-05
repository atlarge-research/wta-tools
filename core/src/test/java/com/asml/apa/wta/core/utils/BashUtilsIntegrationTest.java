package com.asml.apa.wta.core.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BashUtilsIntegrationTest {
    @Test
    void runExecuteCommandSuccessfully() {
        BashUtils bashUtils = new BashUtils();
        CompletableFuture<String> actual = bashUtils.executeCommand("echor hello");

        Throwable exception = assertThrows(BashUtils.BashCommandExecutionException.class, () -> {
            actual.get(); // Trigger the exception by accessing the result
        });

        String expectedErrorMessage = "Bash command execution failed with exit code"; // Replace with the expected error message

        assertEquals(expectedErrorMessage, exception.getMessage());
    }
    @Test
    void runExecuteCommandUnsuccessfully() throws ExecutionException, InterruptedException {
        BashUtils bashUtils = new BashUtils();
        CompletableFuture<String> actual = bashUtils.executeCommand("echo hello");
        assertEquals(actual.get(), "hello\n");
    }
}
