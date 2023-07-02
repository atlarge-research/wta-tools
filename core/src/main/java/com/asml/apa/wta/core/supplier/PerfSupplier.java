package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.util.ShellRunner;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * PerfDataSource class for the performance analyzing tool in Linux.
 *
 * @author Atour Mousavi Gourabi
 * @author Pil Kyu Cho
 * @since 1.0.0
 */
@Slf4j
public class PerfSupplier implements InformationSupplier<PerfDto> {

  private final ShellRunner shellRunner;

  private final boolean isAvailable;

  /**
   * Constructs a {@code perf} data source.
   *
   * @param     shellRunner the {@link ShellRunner} to inject.
   */
  public PerfSupplier(ShellRunner shellRunner) {
    this.shellRunner = shellRunner;
    this.isAvailable = isAvailable();
  }

  /**
   * Verifies the availability of the perf energy data source.
   *
   * @return    {@code boolean} indicating the availability of this data source.
   */
  @Override
  public boolean isAvailable() {
    if (!System.getProperty("os.name").toLowerCase().contains("linux")) {
      return false;
    }
    try {
      if (shellRunner
          .executeCommand("perf list | grep -w 'power/energy-pkg/' | awk '{print $1}'", true)
          .get()
          .equals("power/energy-pkg/")) {
        return true;
      }
      return false;
    } catch (Exception e) {
      log.error("Something went wrong while trying to execute the shell command.");
      return false;
    }
  }

  /**
   * Uses the Perf dependency to get energy metrics (computed asynchronously).
   *
   * @return      if Perf is available, {@link Optional} {@link PerfDto} wrapped in a {@link CompletableFuture} that
   *              will be sent to the driver. Otherwise {@link CompletableFuture} with an empty {@link Optional}.
   */
  @Override
  public CompletableFuture<Optional<PerfDto>> getSnapshot() {
    if (!isAvailable) {
      return notAvailableResult();
    }
    return gatherMetrics().handle((value, exception) -> {
      if (exception != null || value == null) {
        return Optional.empty();
      } else {
        try {
          return Optional.of(
              PerfDto.builder().watt(Double.parseDouble(value)).build());
        } catch (NumberFormatException e) {
          log.error("Error occurred while parsing perf energy metrics");
          return Optional.empty();
        }
      }
    });
  }

  /**
   * Gather the perf energy metrics. Returns a completable future string of total joules in a second,
   * which is equivalent to watt.
   *
   * @return    Completable future string of joules used by the CPU package over the past second.
   */
  public CompletableFuture<String> gatherMetrics() {
    return shellRunner.executeCommand(
        "perf stat -e power/energy-pkg/ -a sleep 1 2>&1 | "
            + "grep -oP '^\\s+\\K[0-9]+[,\\.][0-9]+(?=\\s+Joules)' | sed 's/,/./g'",
        false);
  }
}
