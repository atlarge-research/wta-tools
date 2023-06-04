package com.asml.apa.wta.core.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.utils.BashUtils;
import org.junit.jupiter.api.Test;

public class PerfDataSourceIntegrationTest {

  private final BashUtils bashUtils = new BashUtils();

  private final PerfDataSource sut = new PerfDataSource(bashUtils);

  @Test
  void perfEnergyDataSourceIsAvailable() {
    assertThat(sut.isAvailable()).isTrue();
  }

  @Test
  void perfEnergyGatherMetricsSuccessful() {
    assertThat(sut.gatherMetrics()).isGreaterThan(0.0);
  }
}
