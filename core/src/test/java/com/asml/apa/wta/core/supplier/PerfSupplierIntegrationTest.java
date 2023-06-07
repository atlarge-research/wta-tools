package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.condition.OS.LINUX;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.utils.BashUtils;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;

public class PerfSupplierIntegrationTest {

  private final BashUtils bashUtils = new BashUtils();

  private PerfSupplier sut;

  @Test()
  @EnabledOnOs(LINUX)
  void perfEnergyDataSourceIsAvailableDoesNotThrowException() {
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
  void perfEnergyGetSnapshotSuccessful() throws ExecutionException, InterruptedException {
    sut = new PerfSupplier(bashUtils);
    CompletableFuture<PerfDto> result = sut.getSnapshot();
    Awaitility.await().atMost(Duration.ofSeconds(2)).until(result::isDone);
    assertThat(result).isDone();
    if (sut.isAvailable()) {
      assertThat(result.get().getWatt()).isGreaterThan(0.0);
    } else {
      assertThat(result.get().getWatt()).isEqualTo(0.0);
    }
  }
}
