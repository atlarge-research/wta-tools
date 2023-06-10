package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.condition.OS.LINUX;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

public class PerfSupplierIntegrationTest {

  private final BashUtils bashUtils = new BashUtils();

  private PerfSupplier sut;

  @Test()
  @EnabledOnOs(LINUX)
  void perfEnergyDataSourceIsAvailableDoesNotThrowException() {
    sut = new PerfSupplier(bashUtils);
    assertDoesNotThrow(sut::isAvailable);
  }

  @Test()
  @EnabledOnOs(LINUX)
  void perfEnergyDataSourceGatherMetricsDoesNotThrowException() {
    sut = new PerfSupplier(bashUtils);
    assertDoesNotThrow(sut::gatherMetrics);
  }

  @Test()
  @EnabledOnOs(LINUX)
  void perfEnergyDataSourceGetSnapshotDoesNotThrowException() {
    sut = new PerfSupplier(bashUtils);
    assertDoesNotThrow(sut::getSnapshot);
  }

  @Test
  @EnabledOnOs(LINUX)
  void perfEnergyGetSnapshotSuccessful() {
    sut = new PerfSupplier(bashUtils);
    if (sut.isAvailable()) {
      Optional<PerfDto> result = sut.getSnapshot().join();
      assertThat(result.get().getWatt()).isGreaterThan(0.0);
    }
  }
}
