package com.asml.apa.wta.core.datasource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
    assertDoesNotThrow(sut::isAvailable);
  }

  @Test
  @EnabledOnOs(LINUX)
  void perfEnergyGatherMetricsSuccessful() {
    assertDoesNotThrow(sut::gatherMetrics);
  }
}
