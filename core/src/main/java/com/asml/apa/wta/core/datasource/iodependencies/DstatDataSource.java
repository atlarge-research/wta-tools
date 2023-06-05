package com.asml.apa.wta.core.datasource.iodependencies;

import com.asml.apa.wta.core.dto.DstatDataSourceDto;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * DstatDataSource class.
 *
 * @author Lohithsai Yadala Chanchu
 * @since 1.0.0
 */
@Slf4j
public class DstatDataSource {
  private BashUtils bashUtils;
  private boolean isDstatAvailable;

  public DstatDataSource(BashUtils bashUtils) {
    this.bashUtils = bashUtils;
    this.isDstatAvailable = isDstatAvailable();
  }

  /**
   * Uses the Dstat dependency to get io metrics.
   *
   * @param executorId The executorId string that represents the executorId the io information is being received from.
   * @return DstatDataSourceDto object that will be sent to the driver (with the necessary information filled out)
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public DstatDataSourceDto getAllMetrics(String executorId) throws InterruptedException, ExecutionException {
    if (isDstatAvailable) {
      CompletableFuture<String> allMetrics = bashUtils.executeCommand("dstate -cdngy 1 -c 1");

      List<Integer> metrics = extractNumbers(allMetrics.get());

      try {
        return DstatDataSourceDto.builder()
            .totalUsageUsr(metrics.get(0))
            .totalUsageSys(metrics.get(1))
            .totalUsageIdl(metrics.get(2))
            .totalUsageWai(metrics.get(3))
            .totalUsageStl(metrics.get(4))
            .dskRead(metrics.get(5))
            .dskWrite(metrics.get(6))
            .netRecv(metrics.get(7))
            .netSend(metrics.get(8))
            .pagingIn(metrics.get(9))
            .pagingOut(metrics.get(10))
            .systemInt(metrics.get(11))
            .systemCsw(metrics.get(12))
            .executorId(executorId)
            .build();
      } catch (Exception e) {
        log.error(
            "Something went wrong while receiving the dstat bash command outputs. The cause is: {}",
            e.getCause().toString());
      }
    }
    return null;
  }

  public static List<Integer> extractNumbers(String input) {
    List<Integer> numbers = new ArrayList<>();
    Pattern pattern = Pattern.compile("\\b(\\d+)(k)?\\b");
    Matcher matcher = pattern.matcher(input);

    while (matcher.find()) {
      String match = matcher.group(1);
      boolean isKilo = matcher.group(2) != null;

      int number;
      if (isKilo) {
        number = Integer.parseInt(match) * 1000;
      } else {
        number = Integer.parseInt(match);
      }

      numbers.add(number);
    }

    return numbers;
  }

  /**
   * Checks if the dstat datasource is available.
   *
   * @return A boolean that represents if the dstat datasource is available
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  public boolean isDstatAvailable() {
    try {
      if (bashUtils.executeCommand("dstat -cdngy 1 -c 1").get() != null) {
        return true;
      }
    } catch (InterruptedException | ExecutionException e) {
      log.error(
          "Something went wrong while receiving the dstat bash command outputs. The cause is: {}",
          e.getCause().toString());
      return false;
    }
    log.info("System does not have the necessary dependencies (sysstat) to run dstat.");
    return false;
  }
}
