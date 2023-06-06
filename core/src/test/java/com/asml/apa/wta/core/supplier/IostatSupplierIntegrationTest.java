package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.dto.IostatDto;
import com.asml.apa.wta.core.utils.BashUtils;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

public class IostatSupplierIntegrationTest {
  @Test
  public void IostatSupplierSuccessfullyReturnsADtoObject() throws ExecutionException, InterruptedException {
    IostatSupplier a = new IostatSupplier(new BashUtils());
    var actual = a.getSnapshot().get();
    if (a.isAvailable()) {
      assertTrue(actual instanceof IostatDto);
    } else {
      assertNull(actual);
    }
  }
}
