package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.utils.BashUtils;
import com.asml.apa.wta.core.dto.DstatDto;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class DstatSupplierIntegrationTest {
  @Test
  public void DstatDataSourceSuccessfullyReturnsADtoObject() throws ExecutionException, InterruptedException {
    DstatSupplier a = new DstatSupplier(new BashUtils());
    assertTrue(a.getAllMetrics("x1") instanceof DstatDto);
  }
}
