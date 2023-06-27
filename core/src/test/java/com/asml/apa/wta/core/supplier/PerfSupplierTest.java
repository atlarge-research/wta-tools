package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.dto.PerfDto;
import com.asml.apa.wta.core.util.ShellRunner;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class PerfSupplierTest {

  private final ShellRunner shellRunner = mock(ShellRunner.class);

  private PerfSupplier sut = spy(new PerfSupplier(shellRunner));

  private final String isAvailableBashCommand = "perf list | grep -w 'power/energy-pkg/' | awk '{print $1}'";

  private final String getEnergyMetricsBashCommand = "perf stat -e power/energy-pkg/ -a sleep 1 2>&1 | "
      + "grep -oP '^\\s+\\K[0-9]+[,\\.][0-9]+(?=\\s+Joules)' | sed 's/,/./g'";

  private final CompletableFuture<String> nullCompletableFuture = CompletableFuture.completedFuture(null);

  @Test
  void perfEnergyDataSourceIsAvailable() {
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      when(shellRunner.executeCommand(isAvailableBashCommand, true))
          .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
      assertThat(sut.isAvailable()).isTrue();
    } else {
      assertThat(sut.isAvailable()).isFalse();
    }
  }

  @Test
  void perfIsAvailableThrowsNullPointerException() {
    when(shellRunner.executeCommand(isAvailableBashCommand, true)).thenReturn(nullCompletableFuture);
    assertThat(sut.isAvailable()).isFalse();
  }

  @Test
  void perfEnergyDataSourceNoPowerPkg() {
    when(shellRunner.executeCommand(isAvailableBashCommand, true))
        .thenReturn(CompletableFuture.completedFuture(""));
    assertThat(sut.isAvailable()).isFalse();
  }

  @Test
  void perfEnergyGatherMetricsSuccessful() {
    when(shellRunner.executeCommand(getEnergyMetricsBashCommand, false))
        .thenReturn(CompletableFuture.completedFuture("12.34"));
    String result = sut.gatherMetrics().join();
    assertThat(result).isEqualTo("12.34");
  }

  @Test
  void perfEnergyGatherMetricsCommandErrorReturnsNull() {
    when(shellRunner.executeCommand(getEnergyMetricsBashCommand, false)).thenReturn(nullCompletableFuture);
    String result = sut.gatherMetrics().join();
    assertThat(result).isNull();
  }

  @Test
  void notAvailablePerfReturnsNotAvailableResult() {
    when(shellRunner.executeCommand(isAvailableBashCommand, true)).thenReturn(nullCompletableFuture);
    sut = spy(new PerfSupplier(shellRunner));
    assertThat(sut.isAvailable()).isFalse();
    Optional<PerfDto> result = sut.getSnapshot().join();
    assertThat(result).isEmpty();
  }

  @Test
  void isAvailablePerfReturnsPerfDtoCompletableFuture() {
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      when(shellRunner.executeCommand(isAvailableBashCommand, true))
          .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
      when(shellRunner.executeCommand(getEnergyMetricsBashCommand, false))
          .thenReturn(CompletableFuture.completedFuture("12.34"));
      sut = spy(new PerfSupplier(shellRunner));
      assertThat(sut.isAvailable()).isTrue();
    }
    Optional<PerfDto> result = sut.getSnapshot().join();
    if (sut.isAvailable()) {
      assertThat(result.get().getWatt()).isEqualTo(12.34);
    } else {
      assertEquals(Optional.empty(), result);
    }
  }

  @Test
  void perfEnergyGetSnapshotNullValueReturnsZero() {
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      when(shellRunner.executeCommand(isAvailableBashCommand, true))
          .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
      when(shellRunner.executeCommand(getEnergyMetricsBashCommand, false)).thenReturn(nullCompletableFuture);
      sut = spy(new PerfSupplier(shellRunner));
      assertThat(sut.isAvailable()).isTrue();
    } else {
      sut = spy(new PerfSupplier(shellRunner));
      assertThat(sut.isAvailable()).isFalse();
    }
    Optional<PerfDto> result = sut.getSnapshot().join();
    assertThat(result).isEmpty();
  }

  @Test
  void perfEnergyGetSnapshotCommaDecimalStringReturnsZero() {
    if (System.getProperty("os.name").toLowerCase().contains("linux")) {
      when(shellRunner.executeCommand(isAvailableBashCommand, true))
          .thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
      when(shellRunner.executeCommand(getEnergyMetricsBashCommand, false))
          .thenReturn(CompletableFuture.completedFuture("12,34"));
      sut = spy(new PerfSupplier(shellRunner));
      assertThat(sut.isAvailable()).isTrue();
    } else {
      sut = spy(new PerfSupplier(shellRunner));
      assertThat(sut.isAvailable()).isFalse();
    }
    Optional<PerfDto> result = sut.getSnapshot().join();
    assertThat(result).isEmpty();
  }
}
