package com.asml.apa.wta.core.supplier;

import static org.junit.jupiter.api.Assertions.*;

import com.asml.apa.wta.core.utils.BashUtils;
import org.junit.jupiter.api.Test;

public class IostatSupplierIntegrationTest {

  @Test
  public void IostatSupplierSuccessfullyReturnsADtoObject() {
    IostatSupplier sut = new IostatSupplier(new BashUtils());
    if (sut.isAvailable()) {
      assertDoesNotThrow(() -> sut.getSnapshot().join());
    }
  }
}
