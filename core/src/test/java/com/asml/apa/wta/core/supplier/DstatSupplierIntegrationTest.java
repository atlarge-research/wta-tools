package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class DstatSupplierIntegrationTest {
  @Test
  public void DstatDataSourceSuccessfullyReturnsADtoObject() throws ExecutionException, InterruptedException {
    DstatSupplier a = new DstatSupplier(new BashUtils());
    CompletableFuture<DstatDto> actual = a.getSnapshot();
    assertTrue(actual.get() instanceof DstatDto);
  }
}
