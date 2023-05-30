package com.asml.apa.wta.core.datasource.iodependencies;

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
}
