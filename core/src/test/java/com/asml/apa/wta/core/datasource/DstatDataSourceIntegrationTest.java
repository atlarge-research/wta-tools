package com.asml.apa.wta.core.datasource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.datasource.iodependencies.BashUtils;
import com.asml.apa.wta.core.datasource.iodependencies.DstatDataSource;
import com.asml.apa.wta.core.dto.DstatDataSourceDto;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class DstatDataSourceIntegrationTest {
  @Test
  public void DstatDataSourceSuccessfullyReturnsADtoObject() throws ExecutionException, InterruptedException {
    DstatDataSource a = new DstatDataSource(new BashUtils());
    assertTrue(a.getAllMetrics("x1") instanceof DstatDataSourceDto);
  }
}
