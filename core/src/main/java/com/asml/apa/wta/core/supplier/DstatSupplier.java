package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
public class DstatSupplier implements InformationSupplier<DstatDto> {
  private final ShellUtils shellUtils;
  private final boolean isDstatAvailable;

  public DstatSupplier(ShellUtils shellUtils) {
    this.shellUtils = shellUtils;
    this.isDstatAvailable = isAvailable();
  }

  /**
   * Uses the Dstat dependency to get io metrics.
   *
   * @return DstatDataSourceDto object that will be sent to the driver (with the necessary information filled out)
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @Override
  public CompletableFuture<Optional<DstatDto>> getSnapshot() {
    if (!isDstatAvailable) {
      return notAvailableResult();
    }

    CompletableFuture<String> allMetrics = shellUtils.executeCommand("dstat -cdngy 1 1");

    return allMetrics.thenApply(result -> {
      if (result != null) {
        List<Integer> metrics = extractNumbers(result);
        try {
          return Optional.of(DstatDto.builder()
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
              .build());
        } catch (Exception e) {
          log.error("Something went wrong while receiving the dstat bash command outputs.");
        }
      }
      return Optional.empty();
    });
  }

  /**
   * Parse dstat terminal output.
   *
   * @return List of the parsed numbers from the dstat terminal output
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private static List<Integer> extractNumbers(String input) {
    List<Integer> numbers = new ArrayList<>();
    Pattern pattern = Pattern.compile("\\b(\\d+)(k|B)?\\b");
    Matcher matcher = pattern.matcher(input);

    while (matcher.find()) {
      String match = matcher.group(1);
      String suffix = matcher.group(2);

      int number = Integer.parseInt(match);
      if (suffix != null) {
        if (suffix.equals("k")) {
          number *= 1000;
        } else if (suffix.equals("B")) {
          number *= 1;
        }
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
  @Override
  public boolean isAvailable() {
    try {
      if (shellUtils.executeCommand("dstat -cdngy 1 1").get() != null) {
        return true;
      }
    } catch (InterruptedException | ExecutionException e) {
      log.error("Something went wrong while receiving the dstat shell command outputs.");
      return false;
    }
    log.info("System does not have the necessary dependencies to run dstat.");
    return false;
  }
}
