package com.asml.apa.wta.core.datasource;

import com.asml.apa.wta.core.utils.BashUtils;
import com.asml.apa.wta.core.exceptions.BashCommandExecutionException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class PerfDataSourceTest {

  private final BashUtils bashUtils = mock(BashUtils.class);

  private final PerfDataSource sut = spy(new PerfDataSource(bashUtils));

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
}
