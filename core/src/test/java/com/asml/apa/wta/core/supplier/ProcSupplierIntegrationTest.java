package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.asml.apa.wta.core.dto.ProcDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ProcSupplierIntegrationTest {
  @Test
  @Timeout(value = 10000L, unit = TimeUnit.MILLISECONDS)
  public void procSuccesfullyReturnsDtoObject() {
    ProcSupplier a = new ProcSupplier(new ShellUtils());
    var actual = a.getSnapshot().join();
    if (a.isAvailable()) {
      assertTrue(actual instanceof ProcDto);
    } else {
      assertNull(actual);
    }
  }
}
