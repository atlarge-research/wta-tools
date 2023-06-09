package com.asml.apa.wta.core.supplier;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.util.concurrent.CompletableFuture;

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

  private final ShellUtils shellUtils;

  private final boolean isAvailable;

  /**
   * Constructs a {@code perf} data source.
   *
   * @param shellUtils the {@link ShellUtils} to inject.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public PerfSupplier(ShellUtils shellUtils) {
    this.shellUtils = shellUtils;
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
      return shellUtils
          .executeCommand("perf list | grep -w 'power/energy-pkg/' | awk '{print $1}'")
          .get()
          .equals("power/energy-pkg/");
    } catch (Exception e) {
      log.error("Something went wrong while trying to execute the shell command.");
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
    return gatherMetrics().handle((value, exception) -> {
      if (exception != null || value == null) {
        return PerfDto.builder().watt(0.0).build();
      } else {
        try {
          return PerfDto.builder().watt(Double.parseDouble(value)).build();
        } catch (NumberFormatException e) {
          log.error("Error occurred while parsing perf energy metrics");
          return PerfDto.builder().watt(0.0).build();
        }
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
    return shellUtils.executeCommand("perf stat -e power/energy-pkg/ -a sleep 1 2>&1 | "
        + "grep -oP '^\\s+\\K[0-9]+[,\\.][0-9]+(?=\\s+Joules)' | sed 's/,/./g'");
  }
}
