package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  private final BashUtils bashUtils;

  private boolean isAvailable;

  private static final int NUMBER_OF_IOSTAT_METRICS = 7;

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
      log.error("Something went wrong while receiving the iostat bash command outputs.");
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

    CompletableFuture<String> allMetrics = bashUtils.executeCommand("iostat -d");

    return allMetrics.thenApply(result -> {
      if (result != null) {
        List<OutputLine> rows = parseIostat(result);
        double[] metrics = aggregateIostat(rows);

        try {
          return IostatDto.builder()
              .tps(metrics[0])
              .kiloByteReadPerSec(metrics[1])
              .kiloByteWrtnPerSec(metrics[2])
              .kiloByteDscdPerSec(metrics[3])
              .kiloByteRead(metrics[4])
              .kiloByteWrtn(metrics[5])
              .kiloByteDscd(metrics[6])
              .build();
        } catch (Exception e) {
          log.error("Something went wrong while receiving the iostat bash command outputs.");
        }
      }
      return null;
    });
  }

  /**
   * Sums the respective fields of each column and returns the aggregated result.
   *
   * @param input output of the iostat command
   * @return Parsed ouptut of the iostat command
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private List<OutputLine> parseIostat(String input) {
    List<OutputLine> rows =
        Arrays.stream(input.split("\n")).skip(1).map(OutputLine::new).collect(Collectors.toList());

    return rows;
  }

  /**
   * Sums the respective fields of each column and returns the aggregated result.
   *
   * @param rows List of output rows
   * @return Aggregated array of doubles
   * @author Lohithsai Yadala Chanchu
   * @since 1.0.0
   */
  private double[] aggregateIostat(List<OutputLine> rows) {
    if (rows.size() != 0) {
      return IntStream.range(1, rows.get(0).getRowSize())
          .mapToDouble(j -> rows.stream()
              .mapToDouble(
                  row -> Double.parseDouble(row.getMetricAt(j).replace(',', '.')))
              .sum())
          .toArray();
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
     * @return The size of the outputLine
     * @author Lohithsai Yadala Chanchu
     * @since 1.0.0
     */
    public int getRowSize() {
      return this.outputLine.size();
    }

    /**
     * Returns the row size of the output line.
     *
     * @param index the string that is at the specified index
     * @return The string at the specified index in outputLine
     * @author Lohithsai Yadala Chanchu
     * @since 1.0.0
     */
    public String getMetricAt(int index) {
      return this.outputLine.get(index);
    }
  }
}
