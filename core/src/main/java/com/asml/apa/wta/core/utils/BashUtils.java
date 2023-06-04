package com.asml.apa.wta.core.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BashUtils {

  /**
   * Executes given bash command and returns the terminal output.
   *
   * @param command The bash command string that is run.
   * @return CompletableFuture that returns the output of the command
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public CompletableFuture<String> executeCommand(String command) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        String[] commands = {"bash", "-c", command};
        Process process = new ProcessBuilder(commands).start();
        process.waitFor();

        return readProcessOutput(process);
      } catch (Exception e) {
        log.error(
            "Something went wrong while trying to execute the bash command. The cause is: {}",
            e.getCause().toString());
        throw new BashCommandExecutionException("Error executing bash command", e);
      }
    });
  }

  public class BashCommandExecutionException extends RuntimeException {
    public BashCommandExecutionException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Reads the terminal output.
   *
   * @return String that is the terminal output
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private String readProcessOutput(Process process) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      return reader.readLine();
    } catch (IOException e) {
      log.error(
          "Something went wrong while trying to read bash command outputs. The cause is: {}",
          e.getCause().toString());
      throw new BashCommandExecutionException("Error reading bash output", e);
    }
  }
}
