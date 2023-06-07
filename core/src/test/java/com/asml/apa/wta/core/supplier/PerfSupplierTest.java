package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.awaitility.Awaitility;
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
  void perfEnergyGatherMetricsSuccessful() throws ExecutionException, InterruptedException {
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand))
        .thenReturn(CompletableFuture.completedFuture("12.34"));
    assertThat(sut.gatherMetrics().get()).isEqualTo("12.34");
  }

  @Test
  void perfEnergyGatherMetricsCommandErrorReturnsNull() throws ExecutionException, InterruptedException {
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand)).thenReturn(nullCompletableFuture);
    CompletableFuture<String> result = sut.gatherMetrics();
    assertThat(result.get()).isNull();
  }

  @Test
  void notAvailablePerfReturnsNotAvailableResult() throws ExecutionException, InterruptedException {
    when(bashUtils.executeCommand(isAvailableBashCommand)).thenReturn(nullCompletableFuture);
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isFalse();
    CompletableFuture<PerfDto> result = sut.getSnapshot();
    assertThat(result.isDone()).isTrue();
    assertThat(result.get()).isNull();
  }

  @Test
  void isAvailablePerfReturnsPerfDtoCompletableFuture() throws ExecutionException, InterruptedException {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand))
        .thenReturn(CompletableFuture.completedFuture("12.34"));
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isTrue();
    CompletableFuture<PerfDto> result = sut.getSnapshot();
    Awaitility.await().atMost(Duration.ofSeconds(2)).until(result::isDone);
    assertThat(result.get().getWatt()).isEqualTo(12.34);
  }

  @Test
  void perfEnergyGetSnapshotNullValueReturnsZero() throws ExecutionException, InterruptedException {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand)).thenReturn(nullCompletableFuture);
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isTrue();
    CompletableFuture<PerfDto> result = sut.getSnapshot();
    Awaitility.await().atMost(Duration.ofSeconds(2)).until(result::isDone);
    assertThat(result.get().getWatt()).isEqualTo(0.0);
  }

  @Test
  void perfEnergyGetSnapshotCommaDecimalStringReturnsZero() throws ExecutionException, InterruptedException {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand))
        .thenReturn(CompletableFuture.completedFuture("12,34"));
    sut = spy(new PerfSupplier(bashUtils));
    assertThat(sut.isAvailable()).isTrue();
    CompletableFuture<PerfDto> result = sut.getSnapshot();
    Awaitility.await().atMost(Duration.ofSeconds(2)).until(result::isDone);
    assertThat(result.get().getWatt()).isEqualTo(0.0);
  }
}
