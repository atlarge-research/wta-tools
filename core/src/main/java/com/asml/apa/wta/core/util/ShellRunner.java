package com.asml.apa.wta.core.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to run shell commands.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Slf4j
public class ShellRunner {

  /**
   * Executes given shell command and returns the terminal output.
   *
   * @param command The shell command string that is run.
   * @param suppressErrors Decides if logs written on error
   * @return {@link CompletableFuture} that returns the output of the command
   */
  public CompletableFuture<String> executeCommand(String command, boolean suppressErrors) {
    log.trace("Executing shell command: {}.", command);
    return CompletableFuture.supplyAsync(() -> {
      try {
        String[] commands = {"sh", "-c", command};
        Process process = new ProcessBuilder(commands).start();
        int exitValue = process.waitFor();

        if (exitValue != 0) {
          if (!suppressErrors) {
            log.error(
                "Shell command {} failed to execute correctly with exit code: {}.", command, exitValue);
          }
          return null;
        }

        return readProcessOutput(process);
      } catch (Exception e) {
        if (!suppressErrors) {
          log.error("Something went wrong while trying to execute the shell command.");
        }
        return null;
      }
    });
  }

  /**
   * Reads the terminal output.
   *
   * @return String that is the terminal output
   */
  private String readProcessOutput(Process process) {
    log.trace("Reading shell command outputs.");
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      StringBuilder output = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        output.append(line).append(System.lineSeparator());
      }
      return output.toString();
    } catch (IOException e) {
      log.error("Something went wrong while trying to read shell command outputs.");
      return null;
    }
  }
}
