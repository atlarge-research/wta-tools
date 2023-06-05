package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
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

  private final PerfSupplier sut = new PerfSupplier(bashUtils);

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

  @Test
  @EnabledOnOs(LINUX)
  void perfEnergySnapshotSuccessful() throws ExecutionException, InterruptedException {
    assertThat(sut.isAvailable()).isTrue();
    CompletableFuture<PerfDto> result = sut.getSnapshot();
    Awaitility.await().atMost(Duration.ofSeconds(2)).until(result::isDone);
    assertThat(result.get().getWatt()).isGreaterThanOrEqualTo(0.0);
  }
}
