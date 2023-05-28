package com.asml.apa.wta.core.datasource.iodependencies;

import com.asml.apa.wta.core.dto.IostatDataSourceDto;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * IostatDataSource class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Slf4j
public class IostatDataSource extends BaseIoDependency {

  /**
   * Uses the Iostat dependency to get io metrics .
   *
   * @param executorId The executorId string that represents the executorId the io information is being received from.
   * @return IostatDataSourceDto object that will be sent to the driver (with the necessary information filled out)
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public IostatDataSourceDto getAllMetrics(String executorId) throws IOException, InterruptedException {
    CompletableFuture<String> tpsFuture = executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $2}'");
    CompletableFuture<String> kiloByteReadPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $3}'");
    CompletableFuture<String> kiloByteWrtnPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $4}'");
    CompletableFuture<String> kiloByteDscdPerSecFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $5}'");
    CompletableFuture<String> kiloByteReadFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $6}'");
    CompletableFuture<String> kiloByteWrtnFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $7}'");
    CompletableFuture<String> kiloByteDscdFuture =
        executeCommand("iostat -d | awk '$1 == \"sdc\"' | awk '{print $8}'");
    try {
      return IostatDataSourceDto.builder()
          .tps(Double.parseDouble(tpsFuture.get()))
          .kiloByteReadPerSec(Double.parseDouble(kiloByteReadPerSecFuture.get()))
          .kiloByteWrtnPerSec(Double.parseDouble(kiloByteWrtnPerSecFuture.get()))
          .kiloByteDscdPerSec(Double.parseDouble(kiloByteDscdPerSecFuture.get()))
          .kiloByteRead(Double.parseDouble(kiloByteReadFuture.get()))
          .kiloByteWrtn(Double.parseDouble(kiloByteWrtnFuture.get()))
          .kiloByteDscd(Double.parseDouble(kiloByteDscdFuture.get()))
          .executorId(executorId)
          .build();
    } catch (Exception e) {
      log.error(
          "Something went wrong while receiving the iostat bash command outputs. The cause is: {}",
          e.getCause().toString());
    }
    return null;
  }
}
