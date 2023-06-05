package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * Supplier for Iostat information.
 *
 * @author Lohithsai Yadala Chanchu
 * @author Henry Page
 * @since 1.0.0
 */
@Slf4j
public class IostatSupplier implements InformationSupplier<IostatDto> {

  private final BashUtils bashUtils;

  private boolean isAvailable;

  /**
   * Constructs the supplier with a given instance of bash utils.
   *
   * @param bashUtils The bash utils instance to use
   * @author Henry Page
   * @since 1.0.0
   */
  public IostatSupplier(BashUtils bashUtils) {
    this.bashUtils = bashUtils;
    this.isAvailable = isAvailable();
  }

  /**
   * Checks if the supplier is available.
   *
   * @return A boolean that represents if the iostat supplier is available
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public boolean isAvailable() {
    try {
      if (bashUtils.executeCommand("iostat").get() != null) {
        return true;
      }
    } catch (InterruptedException | ExecutionException e) {
      log.error(
          "Something went wrong while receiving the iostat bash command outputs. The cause is: {}",
          e.getCause().toString());
      return false;
    }
    log.info("System does not have the necessary dependencies (sysstat) to run iostat.");
    return false;
  }

  /**
   * Uses the iostat dependency to get io metrics (computed asynchronously).
   *
   * @return IostatDto object that will be sent to the driver (with the necessary information filled out)
   * @author Lohithsai Yadala Chanchu
   * @author Henry Page
   * @since 1.0.0
   */
  @Override
  public CompletableFuture<IostatDto> getSnapshot() {
    if (!this.isAvailable) {
      return notAvailableResult();
    }

    CompletableFuture<String> allMetrics = bashUtils.executeCommand("iostat -d | awk '$1 == \"sdc\"'");

    return allMetrics.thenApply(result -> {
      String[] metrics = result.trim().split("\\s+");

      try {
        return IostatDto.builder()
            .tps(Double.parseDouble(metrics[1]))
            .kiloByteReadPerSec(Double.parseDouble(metrics[2]))
            .kiloByteWrtnPerSec(Double.parseDouble(metrics[3]))
            .kiloByteDscdPerSec(Double.parseDouble(metrics[4]))
            .kiloByteRead(Double.parseDouble(metrics[5]))
            .kiloByteWrtn(Double.parseDouble(metrics[6]))
            .kiloByteDscd(Double.parseDouble(metrics[7]))
            .build();
      } catch (Exception e) {
        log.error(
            "Something went wrong while receiving the iostat bash command outputs. The cause is: {}",
            e.getCause().toString());
      }
      return null;
    });
  }
}
