package com.asml.apa.wta.core.datasource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.datasource.iodependencies.BashUtils;
import com.asml.apa.wta.core.datasource.iodependencies.DstatDataSource;
import com.asml.apa.wta.core.dto.DstatDataSourceDto;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class DstatDataSourceTest {
  @Test
  public void w() throws ExecutionException, InterruptedException {
    DstatDataSource a = new DstatDataSource(new BashUtils());
    var q = a.getAllMetrics("x1");
    assertTrue(q instanceof DstatDataSourceDto);
  }
}
