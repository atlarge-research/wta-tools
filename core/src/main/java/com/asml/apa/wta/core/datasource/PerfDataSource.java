package com.asml.apa.wta.core.datasource;

import com.asml.apa.wta.core.exceptions.BashCommandExecutionException;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * PerfDataSource class.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
@Slf4j
public class PerfDataSource {

  private final BashUtils bashUtils;

  /**
   * Constructs a {@code perf} data source.
   *
   * @param bashUtils the {@link BashUtils} to inject.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public PerfDataSource(BashUtils bashUtils) {
    this.bashUtils = bashUtils;
  }

  /**
   * Verifies the availability of the perf energy data source.
   *
   * @return a {@code boolean} indicating the availability of this data source
   * @author Atour Mousavi Gourabi
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public boolean isAvailable() {
    try {
      return bashUtils
          .executeCommand("perf list | grep -w 'power/energy-pkg/' | awk '{print $1}'")
          .get()
          .equals("power/energy-pkg/");
    } catch (BashCommandExecutionException | ExecutionException | InterruptedException | NullPointerException e) {
      log.error("Something went wrong while trying to execute the bash command.");
      return false;
    }
  }

  /**
   * Gather the perf energy metrics.
   *
   * @return the joules used by the CPU package over the past second
   * @author Atour Mousavi Gourabi
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public double gatherMetrics() {
    try {
      CompletableFuture<String> energyMetrics = bashUtils.executeCommand(
          "perf stat -e power/energy-pkg/ -a sleep 1 2>&1 | grep -oP '^\\s+\\K[\\d,]+(?=\\s+Joules)' | sed 's/,/./g'");
      return Double.parseDouble(energyMetrics.get());
    } catch (NumberFormatException e) {
      throw new NumberFormatException("The captured string can not be parsed into a double.");
    } catch (BashCommandExecutionException | NullPointerException | InterruptedException | ExecutionException e) {
      log.error("Error occurred while gathering perf energy metrics");
      return 0.0;
    }
  }
}
