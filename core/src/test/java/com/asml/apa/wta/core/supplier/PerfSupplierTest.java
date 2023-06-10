package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class PerfSupplierTest {

  private final BashUtils bashUtils = mock(BashUtils.class);

  private PerfSupplier sut = spy(new PerfSupplier(bashUtils));

  private final String isAvailableBashCommand = "perf list | grep -w 'power/energy-pkg/' | awk '{print $1}'";

  private final String getEnergyMetricsBashCommand = "perf stat -e power/energy-pkg/ -a sleep 1 2>&1 | "
      + "grep -oP '^\\s+\\K[0-9]+[,\\.][0-9]+(?=\\s+Joules)' | sed 's/,/./g'";

  private final CompletableFuture<String> nullCompletableFuture = CompletableFuture.completedFuture(null);

  @Test
  void perfEnergyDataSourceIsAvailable() {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    assertThat(sut.isAvailable()).isTrue();
  }

  @Test
  void perfIsAvailableThrowsNullPointerException() {
    when(bashUtils.executeCommand(isAvailableBashCommand)).thenReturn(nullCompletableFuture);
    assertThat(sut.isAvailable()).isFalse();
  }

  @Test
  void perfEnergyDataSourceNoPowerPkg() {
    when(bashUtils.executeCommand(isAvailableBashCommand)).thenReturn(CompletableFuture.completedFuture(""));
    assertThat(sut.isAvailable()).isFalse();
  }

  @Test
  void perfEnergyGatherMetricsSuccessful() {
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand))
        .thenReturn(CompletableFuture.completedFuture("12.34"));
    String result = sut.gatherMetrics().join();
    assertThat(result).isEqualTo("12.34");
  }

  @Test
  void perfEnergyGatherMetricsCommandErrorReturnsNull() {
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand)).thenReturn(nullCompletableFuture);
    String result = sut.gatherMetrics().join();
    assertThat(result).isNull();
  }

  @Test
  void notAvailablePerfReturnsNotAvailableResult() {
    when(bashUtils.executeCommand(isAvailableBashCommand)).thenReturn(nullCompletableFuture);
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isFalse();
    Optional<PerfDto> result = sut.getSnapshot().join();
    assertThat(result).isEmpty();
  }

  @Test
  void isAvailablePerfReturnsPerfDtoCompletableFuture() {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand))
        .thenReturn(CompletableFuture.completedFuture("12.34"));
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isTrue();
    Optional<PerfDto> result = sut.getSnapshot().join();
    assertThat(result.get().getWatt()).isEqualTo(12.34);
  }

  @Test
  void perfEnergyGetSnapshotNullValueReturnsZero() {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand)).thenReturn(nullCompletableFuture);
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isTrue();
    Optional<PerfDto> result = sut.getSnapshot().join();
    assertThat(result).isEmpty();
  }

  @Test
  void perfEnergyGetSnapshotCommaDecimalStringReturnsZero() {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand))
        .thenReturn(CompletableFuture.completedFuture("12,34"));
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isTrue();
    Optional<PerfDto> result = sut.getSnapshot().join();
    assertThat(result).isEmpty();
  }
}
