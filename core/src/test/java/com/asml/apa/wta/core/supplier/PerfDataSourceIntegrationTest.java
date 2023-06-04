package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.condition.OS.LINUX;

import com.asml.apa.wta.core.utils.BashUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

public class PerfDataSourceIntegrationTest {

  private final BashUtils bashUtils = new BashUtils();

  private final PerfDataSource sut = new PerfDataSource(bashUtils);

  @Test()
  @EnabledOnOs(LINUX)
  void perfEnergyDataSourceIsAvailable() {
    assertThat(sut.isAvailable()).isTrue();
  }

  @Test
  @EnabledOnOs(LINUX)
  void perfEnergyGatherMetricsSuccessful() {
    assertThat(sut.gatherMetrics()).isGreaterThanOrEqualTo(0.0);
  }
}
