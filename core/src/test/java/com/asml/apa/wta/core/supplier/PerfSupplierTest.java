package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.exceptions.BashCommandExecutionException;
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

  @Test
  void perfEnergyDataSourceIsAvailable() {
    when(bashUtils.executeCommand(isAvailableBashCommand))
        .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    assertThat(sut.isAvailable()).isTrue();
  }

  @Test
  void perfEnergyDataSourceNotAvailable() {
    when(bashUtils.executeCommand(isAvailableBashCommand)).thenThrow(BashCommandExecutionException.class);
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
    assertThat(sut.gatherMetrics()).isEqualTo(12.34);
  }

  @Test
  void perfEnergyGatherMetricsCommaDecimalStringThrowsException() {
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand))
        .thenReturn(CompletableFuture.completedFuture("12,34"));
    assertThrows(NumberFormatException.class, sut::gatherMetrics);
  }

  @Test
  void perfEnergyGatherMetricsCommandErrorReturnsDefaultValue() {
    when(bashUtils.executeCommand(getEnergyMetricsBashCommand)).thenThrow(BashCommandExecutionException.class);
    assertThat(sut.gatherMetrics()).isEqualTo(0.0);
  }

  @Test
  void notAvailablePerfReturnsNotAvailableResult() throws ExecutionException, InterruptedException {
    when(bashUtils.executeCommand(isAvailableBashCommand)).thenThrow(BashCommandExecutionException.class);
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
    CompletableFuture<PerfDto> result = sut.getSnapshot();
    Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .until(result::isDone);
    assertThat(result.get().getWatt()).isEqualTo(12.34);
  }
}
