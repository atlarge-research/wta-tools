package com.asml.apa.wta.core.datasource;

import com.asml.apa.wta.core.datasource.iodependencies.BashUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * PerfDataSource class.
 *
 * @author Atour Mousavi Gourabi
 * @since 1.0.0
 */
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
   * @since 1.0.0
   */
  public boolean isAvailable() {
    try {
      return bashUtils
          .executeCommand("perf list | grep -w 'power/energy-pkg/'")
          .get()
          .equals("power/energy-pkg/");
    } catch (ExecutionException | InterruptedException e) {
      return false;
    }
  }

  /**
   * Gather the perf energy metrics.
   *
   * @return the joules used by the CPU package over the past second.
   * @author Atour Mousavi Gourabi
   * @since 1.0.0
   */
  public double gatherMetrics() throws ExecutionException, InterruptedException {
    CompletableFuture<String> energyMetrics = bashUtils.executeCommand(
        "perf stat -e power/energy-pkg/ -a sleep 1 | grep -m 1 -oE '[0-9]+(\\.[0-9]+)?'");
    return Double.parseDouble(energyMetrics.get());
  }
}
