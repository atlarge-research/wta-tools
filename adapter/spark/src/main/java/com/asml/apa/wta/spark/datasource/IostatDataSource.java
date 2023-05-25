package com.asml.apa.wta.spark.datasource;

import com.asml.apa.wta.spark.datasource.dto.IostatDataSourceDto;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;

public class IostatDataSource {

  public IostatDataSourceDto getAllMetrics(String executorId) throws IOException, InterruptedException {
    CompletableFuture<Double> tpsFuture = executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $2}'");
    CompletableFuture<Double> KBReadPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $3}'");
    CompletableFuture<Double> KBWrtnPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $4}'");
    CompletableFuture<Double> KBDscdPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $5}'");
    CompletableFuture<Double> KBReadFuture = executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $6}'");
    CompletableFuture<Double> KBWrtnFuture = executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $7}'");
    CompletableFuture<Double> KBDscdFuture = executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $8}'");
    try {
      return IostatDataSourceDto.builder()
          .tps(tpsFuture.get())
          .KBReadPerSec(KBReadPerSecFuture.get())
          .KBWrtnPerSec(KBWrtnPerSecFuture.get())
          .KBDscdPerSec(KBDscdPerSecFuture.get())
          .KBRead(KBReadFuture.get())
          .KBWrtn(KBWrtnFuture.get())
          .KBDscd(KBDscdFuture.get())
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
