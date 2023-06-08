package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.utils.BashUtils;
import org.junit.jupiter.api.Test;

public class ProcSupplierIntegrationTest {
  @Test
  public void procSuccesfullyReturnsDtoObject() {
    ProcSupplier a = new ProcSupplier(new BashUtils());
    var actual = a.getSnapshot().join();
    if (a.isAvailable()) {
      assertTrue(actual instanceof ProcDto);
    } else {
      assertNull(actual);
    }
  }
}
