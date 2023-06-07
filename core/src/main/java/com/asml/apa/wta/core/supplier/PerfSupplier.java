package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.exceptions.BashCommandExecutionException;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * PerfDataSource class.
 *
 * @author Atour Mousavi Gourabi
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
@Slf4j
public class PerfSupplier implements InformationSupplier<PerfDto> {

  private final BashUtils bashUtils;

  private final boolean isAvailable;

  /**
   * Constructs a {@code perf} data source.
   *
   * @param bashUtils the {@link BashUtils} to inject.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public PerfSupplier(BashUtils bashUtils) {
    this.bashUtils = bashUtils;
    this.isAvailable = isAvailable();
  }

  /**
   * Verifies the availability of the perf energy data source.
   *
   * @return a {@code boolean} indicating the availability of this data source
   * @author Atour Mousavi Gourabi
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @Override
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
   * Uses the Perf dependency to get energy metrics (computed asynchronously).
   *
   * @return PerfDto object that will be sent to the driver (with the necessary information filled out)
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  @Override
  public CompletableFuture<PerfDto> getSnapshot() {
    if (!isAvailable) {
      return notAvailableResult();
    }
    return gatherMetrics()
        .thenApply(result -> {
          try {
            return PerfDto.builder().watt(Double.parseDouble(result)).build();
          } catch (NumberFormatException e) {
            log.error("Error occurred while parsing perf energy metrics");
            return PerfDto.builder().watt(0.0).build();
          }
        });
  }

  /**
   * Gather the perf energy metrics. Returns a completable future string of total joules in a second,
   * which is equivalent to watt.
   *
   * @return Completable future string of joules used by the CPU package over the past second
   * @author Atour Mousavi Gourabi
   * @author Pil Kyu Cho
   * @since 1.0.0
   */
  public CompletableFuture<String> gatherMetrics() {
    try {
      return bashUtils.executeCommand("perf stat -e power/energy-pkg/ -a sleep 1 2>&1 | "
          + "grep -oP '^\\s+\\K[0-9]+[,\\.][0-9]+(?=\\s+Joules)' | sed 's/,/./g'");
    } catch (BashCommandExecutionException | NullPointerException e) {
      log.error("Error occurred while gathering perf energy metrics");
      return CompletableFuture.completedFuture("0.0");
    }
  }
}
