package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.spark.datasource.dto.IostatDataSourceDto;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;

public class IostatDataSource {

  /**
   * Uses the Iostat dependency to get io metrics .
   *
   * @param executorId The executorId string that represents the executorId the io information is being received from.
   * @return IostatDataSourceDto object that will be sent to the driver (with the necessary information filled out)
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public IostatDataSourceDto getAllMetrics(String executorId) throws IOException, InterruptedException {
    CompletableFuture<Double> tpsFuture = executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $2}'");
    CompletableFuture<Double> kiloByteReadPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $3}'");
    CompletableFuture<Double> kiloByteWrtnPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $4}'");
    CompletableFuture<Double> kiloByteDscdPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $5}'");
    CompletableFuture<Double> kiloByteReadFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $6}'");
    CompletableFuture<Double> kiloByteWrtnFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $7}'");
    CompletableFuture<Double> kiloByteDscdFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $8}'");
    try {
      return IostatDataSourceDto.builder()
          .tps(tpsFuture.get())
          .kiloByteReadPerSec(kiloByteReadPerSecFuture.get())
          .kiloByteWrtnPerSec(kiloByteWrtnPerSecFuture.get())
          .kiloByteDscdPerSec(kiloByteDscdPerSecFuture.get())
          .kiloByteRead(kiloByteReadFuture.get())
          .kiloByteWrtn(kiloByteWrtnFuture.get())
          .kiloByteDscd(kiloByteDscdFuture.get())
          .executorId((executorId))
          .build();
    } catch (Exception e) {
      // TODO: Log this as an error
      Throwable cause = e.getCause();
    }
    return null;
  }

  public CompletableFuture<Double> executeCommand(String command) throws InterruptedException, IOException {
    return CompletableFuture.supplyAsync(() -> {
      try {
        String[] commands = {"bash", "-c", command};
        Process process = new ProcessBuilder(commands).start();
        process.waitFor();

        String line = "";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          line = reader.readLine();
        } catch (IOException e) {
          // TODO: Log this as an error
          Throwable cause = e.getCause();
        }

        return Double.parseDouble(line);
      } catch (Exception e) {
        // TODO: Log this as an error
        Throwable cause = e.getCause();
        return -1.0;
      }
    });
  }
}
