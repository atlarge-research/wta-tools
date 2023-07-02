package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.util.ShellRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NoArgsConstructor;
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

  private final ShellRunner shellRunner;

  private boolean isAvailable;

  private static final int NUMBER_OF_IOSTAT_METRICS = 7;

  /**
   * Constructs the supplier with a given instance of shell utils.
   *
   * @param shellRunner     shell utils instance to use.
   */
  public IostatSupplier(ShellRunner shellRunner) {
    this.shellRunner = shellRunner;
    this.isAvailable = isAvailable();
  }

  /**
   * Checks if the supplier is available.
   *
   * @return      boolean that represents if the iostat supplier is available.
   */
  @Override
  public boolean isAvailable() {
    if (!System.getProperty("os.name").toLowerCase().contains("linux")) {
      log.info("The iostat dependency is not available.");
      return false;
    }
    try {
      if (shellRunner.executeCommand("iostat", true).get() != null) {
        return true;
      }
      return false;
    } catch (InterruptedException | ExecutionException e) {
      log.error("Something went wrong while receiving the iostat shell command outputs.");
      return false;
    }
  }

  /**
   * Uses the iostat dependency to get io metrics (computed asynchronously).
   *
   * @return      if Iostat is available, {@link Optional} {@link IostatDto} wrapped in a {@link CompletableFuture} that
   *              will be sent to the driver. Otherwise {@link CompletableFuture} with an empty {@link Optional}.
   */
  @Override
  public CompletableFuture<Optional<IostatDto>> getSnapshot() {
    if (!this.isAvailable) {
      return notAvailableResult();
    }

    CompletableFuture<String> allMetrics = shellRunner.executeCommand("iostat -d", false);

    return allMetrics.thenApply(result -> {
      if (result != null) {
        try {
          List<OutputLine> rows = parseIostat(result);
          double[] metrics = aggregateIostat(rows);

          if (metrics.length == 7) {
            return Optional.of(IostatDto.builder()
                .tps(metrics[0])
                .kiloByteReadPerSec(metrics[1])
                .kiloByteWrtnPerSec(metrics[2])
                .kiloByteDscdPerSec(metrics[3])
                .kiloByteRead(metrics[4])
                .kiloByteWrtn(metrics[5])
                .kiloByteDscd(metrics[6])
                .build());
          }
        } catch (NullPointerException npe) {
          log.error("Iostat returned a malformed output: {}", npe.toString());
        } catch (IndexOutOfBoundsException e) {
          log.error("A different number of iostat metrics were found than expected");
        } catch (NumberFormatException e) {
          log.error("Something went wrong while parsing iostat terminal output");
        } catch (Exception e) {
          log.error("Something went wrong while handling iostat metrics");
        }
      }
      return Optional.empty();
    });
  }

  /**
   * Sums the respective fields of each column and returns the aggregated result.
   *
   * @param input     input of the Iostat command.
   * @return          parsed output of the Iostat command.
   */
  private List<OutputLine> parseIostat(String input) {
    List<OutputLine> rows =
        Arrays.stream(input.split("\n")).skip(1).map(OutputLine::new).collect(Collectors.toList());
    return rows;
  }

  /**
   * Sums the respective fields of each column and returns the aggregated result.
   *
   * @param rows      list of output rows.
   * @return          aggregated array of doubles.
   */
  private double[] aggregateIostat(List<OutputLine> rows) {
    if (rows.size() != 0) {
      double[] aggregatedResult = IntStream.range(1, rows.get(0).getRowSize())
          .mapToDouble(j -> rows.stream()
              .mapToDouble(
                  row -> Double.parseDouble(row.getMetricAt(j).replace(',', '.')))
              .sum())
          .toArray();
      if (aggregatedResult.length == NUMBER_OF_IOSTAT_METRICS) {
        return aggregatedResult;
      }
    }
    return new double[NUMBER_OF_IOSTAT_METRICS];
  }

  /**
   * Container class to be used in nested String Lists.
   *
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  @NoArgsConstructor
  private class OutputLine {
    private List<String> outputLine = new ArrayList<>();

    OutputLine(String line) {
      outputLine = Arrays.asList(line.split("\\s+"));
    }

    /**
     * Returns the row size of the output line.
     *
     * @return      size of the output line.
     */
    public int getRowSize() {
      return this.outputLine.size();
    }

    /**
     * Returns the row size of the output line.
     *
     * @param index     string that is at the specified index.
     * @return          string at the specified index in outputLine.
     */
    public String getMetricAt(int index) {
      return this.outputLine.get(index);
    }
  }
}
