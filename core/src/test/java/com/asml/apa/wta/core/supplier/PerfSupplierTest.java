package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.asml.apa.wta.core.exceptions.BashCommandExecutionException;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class PerfSupplierTest {

  private final BashUtils bashUtils = mock(BashUtils.class);

  private final PerfSupplier sut = spy(new PerfSupplier(bashUtils));

  @Test
  void perfEnergyDataSourceIsAvailable() {
    when(bashUtils.executeCommand(anyString())).thenReturn(CompletableFuture.completedFuture("power/energy-pkg/"));
    assertThat(sut.isAvailable()).isTrue();
  }

  @Test
  void perfEnergyDataSourceNotAvailable() {
    when(bashUtils.executeCommand(anyString())).thenThrow(BashCommandExecutionException.class);
    assertThat(sut.isAvailable()).isFalse();
  }

  @Test
  void perfEnergyDataSourceNoPowerPkg() {
    when(bashUtils.executeCommand(anyString())).thenReturn(CompletableFuture.completedFuture(""));
    assertThat(sut.isAvailable()).isFalse();
  }

  @Test
  void perfEnergyGatherMetricsSuccessful() {
    String energyStringDotDecimal = "12.34";
    when(bashUtils.executeCommand(anyString()))
        .thenReturn(CompletableFuture.completedFuture(energyStringDotDecimal));
    double energy = 12.34;
    assertThat(sut.gatherMetrics()).isEqualTo(energy);
  }

  @Test
  void perfEnergyGatherMetricsCommaDecimalStringThrowsException() {
    String energyStringCommaDecimal = "12,34";
    when(bashUtils.executeCommand(anyString()))
        .thenReturn(CompletableFuture.completedFuture(energyStringCommaDecimal));
    assertThrows(NumberFormatException.class, sut::gatherMetrics);
  }

  @Test
  void perfEnergyGatherMetricsCommandErrorReturnsDefaultValue() {
    when(bashUtils.executeCommand(anyString())).thenThrow(BashCommandExecutionException.class);
    assertThat(sut.gatherMetrics()).isEqualTo(0.0);
  }
}
