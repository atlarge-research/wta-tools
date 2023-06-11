package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import org.junit.jupiter.api.Test;

public class DstatSupplierIntegrationTest {
  @Test
  public void DstatSupplierSuccessfullyReturnsADtoObject() {
    DstatSupplier a = new DstatSupplier(new ShellUtils());
    var actual = a.getSnapshot().join();
    if (a.isAvailable()) {
      assertTrue(actual instanceof DstatDto);
    } else {
      assertNull(actual);
    }
  }
}
