package com.asml.apa.wta.core.supplier;

import static org.assertj.core.api.Assertions.assertThat;

import com.asml.apa.wta.core.dto.DstatDto;
import com.asml.apa.wta.core.utils.ShellUtils;
import org.junit.jupiter.api.Test;

public class DstatSupplierIntegrationTest {
  @Test
  public void DstatSupplierSuccessfullyReturnsADtoObject() {
    DstatSupplier a = new DstatSupplier(new ShellUtils());
    var actual = a.getSnapshot().join();
    if (a.isAvailable()) {
      assertThat(actual).isPresent();
      assertThat(actual.get()).isInstanceOf(DstatDto.class);
    } else {
      assertThat(actual).isEmpty();
    }
  }
}
