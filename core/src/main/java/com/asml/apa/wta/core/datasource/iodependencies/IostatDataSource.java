package com.asml.apa.wta.core.datasource.iodependencies;

import com.asml.apa.wta.core.dto.IostatDataSourceDto;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * IostatDataSource class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Slf4j
public class IostatDataSource {
  private BashUtils bashUtils;
  private boolean isIostatAvailable;

  public IostatDataSource(BashUtils bashUtils) {
    this.bashUtils = bashUtils;
    this.isIostatAvailable = isIostatAvailable();
  }

  /**
   * Uses the Iostat dependency to get io metrics.
   *
   * @param executorId The executorId string that represents the executorId the io information is being received from.
   * @return IostatDataSourceDto object that will be sent to the driver (with the necessary information filled out)
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public IostatDataSourceDto getAllMetrics(String executorId)
      throws IOException, InterruptedException, ExecutionException {
    if (isIostatAvailable) {
      CompletableFuture<String> allMetrics = bashUtils.executeCommand("iostat -d | awk '$1 == \"sdc\"'");

      String[] metrics = allMetrics.get().trim().split("\\s+");

      try {
        return IostatDataSourceDto.builder()
            .tps(Double.parseDouble(metrics[1]))
            .kiloByteReadPerSec(Double.parseDouble(metrics[2]))
            .kiloByteWrtnPerSec(Double.parseDouble(metrics[3]))
            .kiloByteDscdPerSec(Double.parseDouble(metrics[4]))
            .kiloByteRead(Double.parseDouble(metrics[5]))
            .kiloByteWrtn(Double.parseDouble(metrics[6]))
            .kiloByteDscd(Double.parseDouble(metrics[7]))
            .executorId(executorId)
            .build();
      } catch (Exception e) {
        log.error(
            "Something went wrong while receiving the iostat bash command outputs. The cause is: {}",
            e.getCause().toString());
      }
    } else {
      log.info(
          "System is not running on a unix based os. Metrics from the iostat datasource could not be obtained");
    }
    return null;
  }

  /**
   * Checks if the iostat datasource is available.
   *
   * @return A boolean that represents if the iostat datasource is available
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public boolean isIostatAvailable() {
    try {
      if (bashUtils.executeCommand("iostat").get() != null) {
        return true;
      }
    } catch (InterruptedException | ExecutionException e) {
      log.error(
          "Something went wrong while receiving the iostat bash command outputs. The cause is: {}",
          e.getCause().toString());
    }
    log.info("System does not have the necessary dependencies (sysstat) to run iostat.");
    return false;
  }
}
